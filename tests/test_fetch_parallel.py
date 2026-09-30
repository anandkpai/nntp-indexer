import os
import time
import threading
import unittest
from configparser import ConfigParser
from unittest.mock import patch

from nntp_lib.fetch import fetch_headers_chunked, fetch_rows_xover, _fetch_chunk_with_client


class FakeClient:
    def group(self, group):
        return '211', 12, 1, 12, group

    def xover(self, start, end):
        # Make the first chunk slower, and return rows in reverse order to
        # exercise both per-chunk sorting and collection by range.
        if start == 1:
            time.sleep(0.2)
        return '224', [(number, {
            'message-id': f'<{number}@test>',
            'subject': 'test\ud800 subject',
            'from': str(threading.get_ident()),
            'date': 'Tue, 22 Sep 2026 12:00:00 +0000',
            ':bytes': '100', ':lines': '2',
            'xref': str(os.getpid()),
        }) for number in reversed(range(start, end + 1)) if number != 5]

    def quit(self):
        pass


# Importable functions are required for the real spawn-based executor. These
# fixtures run the production fetch/parse code inside the child process.
def fake_fetch_chunk(config, group, start, end):
    rows = fetch_rows_xover(FakeClient(), group, start, end)
    return rows, time.perf_counter()


def failing_fetch_chunk(config, group, start, end):
    if start == 4:
        raise RuntimeError('simulated server failure')
    return fake_fetch_chunk(config, group, start, end)


class ParallelFetchTests(unittest.TestCase):
    def setUp(self):
        self.config = ConfigParser()
        self.config['servers'] = {'max_workers': '2'}

    def fetch(self, worker=fake_fetch_chunk, **kwargs):
        with patch('nntp_lib.fetch.get_nntp_client', return_value=FakeClient()), \
             patch('nntp_lib.fetch._fetch_chunk_with_client', worker):
            return fetch_headers_chunked(self.config, 'test', **kwargs)

    def test_spawn_parsing_ordering_and_missing_articles(self):
        self.config['servers']['fetch_backend'] = 'processes'
        rows = self.fetch(start=12, back_filled_up_to=1, chunk_size=3)
        self.assertEqual([r['artnum'] for r in rows],
                         [n for n in range(1, 13) if n != 5])
        self.assertTrue(all(int(r['xref']) != os.getpid() for r in rows))
        for row in rows:
            self.assertEqual(row['subject'], 'test subject')
            self.assertEqual(row['bytes'], 100)
            self.assertEqual(row['lines'], 2)
            self.assertIsNotNone(row['date_utc'])
            self.assertEqual(row['group_name'], 'test')

    def test_default_threads_share_memory_and_preserve_order(self):
        rows = self.fetch(start=12, back_filled_up_to=1, chunk_size=3)
        self.assertEqual([r['artnum'] for r in rows],
                         [n for n in range(1, 13) if n != 5])
        self.assertTrue(all(int(r['xref']) == os.getpid() for r in rows))
        self.assertTrue(all(int(r['from_addr']) != threading.get_ident() for r in rows))
        self.assertGreater(len({r['from_addr'] for r in rows}), 1)

    def test_unknown_backend_is_rejected(self):
        self.config['servers']['fetch_backend'] = 'invalid'
        with self.assertRaisesRegex(ValueError, 'fetch_backend'):
            self.fetch(start=12, back_filled_up_to=1)

    def test_limit_clips_final_chunk(self):
        rows = self.fetch(start=12, back_filled_up_to=1, chunk_size=3, limit=7)
        self.assertEqual([r['artnum'] for r in rows], [1, 2, 3, 4, 6, 7])

    def test_existing_partial_failure_behavior(self):
        rows = self.fetch(worker=failing_fetch_chunk, start=9,
                          back_filled_up_to=1, chunk_size=3)
        self.assertEqual([r['artnum'] for r in rows], [1, 2, 3, 7, 8, 9])

    def test_empty_range_does_not_start_pool(self):
        with patch('nntp_lib.fetch.ThreadPoolExecutor') as pool:
            self.assertEqual(self.fetch(start=1, back_filled_up_to=2), [])
            pool.assert_not_called()

    def test_invalid_settings(self):
        for workers, chunk_size in [('0', 3), ('2', 0)]:
            self.config['servers']['max_workers'] = workers
            with self.assertRaises(ValueError):
                self.fetch(start=12, back_filled_up_to=1, chunk_size=chunk_size)

    def test_worker_closes_connection_on_parse_error(self):
        client = FakeClient()
        with patch('nntp_lib.fetch.get_nntp_client', return_value=client), \
             patch.object(client, 'xover', side_effect=RuntimeError('failed')), \
             patch.object(client, 'quit') as quit_client:
            with self.assertRaises(RuntimeError):
                _fetch_chunk_with_client(self.config, 'test', 1, 3)
            quit_client.assert_called_once()


if __name__ == '__main__':
    unittest.main()
