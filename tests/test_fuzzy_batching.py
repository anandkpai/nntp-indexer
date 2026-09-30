import sqlite3
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from nntp_lib.db import ensure_db, upsert_headers
from nntp_lib.nzb import create_grouped_nzbs_from_db


class FuzzyBatchingTests(unittest.TestCase):
    def generate(self, specs, minimum=4):
        """Specs contain name, poster, day, emitted size, advertised size."""
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'test.sqlite')
            rows = []
            for index, (name, poster, day, size, total) in enumerate(specs):
                for part in range(1, size + 1):
                    rows.append(dict(
                        subject=f'"{name}.jpg" yEnc ({part}/{total})',
                        from_addr=poster, date_utc=f'2026-01-{day:02d}T00:00:00Z',
                        message_id=f'<{index}-{part}@test>', artnum=len(rows) + 1,
                        bytes=100, refs=None, lines=1, xref=None))
            with sqlite3.connect(path) as conn:
                ensure_db(conn)
                upsert_headers(conn, 'test', rows)
            results = create_grouped_nzbs_from_db(
                path, 'test', directory, fuzzy_grouping=True,
                fuzzy_similarity_threshold=100, min_articles_per_nzb=minimum,
                require_complete_sets=True)
        self.output_names = [name for name, _ in results]
        ns = {'n': 'http://www.newzbin.com/DTD/2003/nzb'}
        batches = [ET.fromstring(xml).findall('n:file', ns) for _, xml in results]
        ids = [segment.text for files in batches for file in files
               for segment in file.findall('n:segments/n:segment', ns)]
        expected = {r['message_id'][1:-1] for index, spec in enumerate(specs)
                    if spec[3] == spec[4] for r in rows
                    if r['message_id'].startswith(f'<{index}-')}
        self.assertEqual(set(ids), expected)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len({name for name, _ in results}), len(results))
        for files in batches:
            for file in files:
                segments = file.findall('n:segments/n:segment', ns)
                self.assertEqual([int(s.get('number')) for s in segments],
                                 list(range(1, len(segments) + 1)))
        return batches

    def names(self, batches):
        return [[file.get('subject').strip('" ') for file in files] for files in batches]

    def test_nearest_name_first_and_final_remainder(self):
        batches = self.generate([(name, 'poster', 1, 2, 2)
                                 for name in ['aaaaaa', 'abbbbb', 'zaaaaa']])
        self.assertEqual(self.names(batches), [['aaaaaa.jpg', 'zaaaaa.jpg'], ['abbbbb.jpg']])

    def test_never_mixes_posters_and_names_are_unique(self):
        batches = self.generate([('same', poster, 1, 2, 2) for poster in ['one', 'two']])
        self.assertEqual(len(batches), 2)
        self.assertEqual(self.output_names, ['same_test.nzb', 'same_test_2.nzb'])
        self.assertEqual([{f.get('poster') for f in files} for files in batches],
                         [{'one'}, {'two'}])

    def test_time_window_skips_nearest_ineligible_name(self):
        batches = self.generate([('aaaaaa', 'poster', 1, 2, 2),
                                 ('aaaaab', 'poster', 10, 2, 2),
                                 ('zaaaaa', 'poster', 3, 2, 2)])
        self.assertEqual(self.names(batches), [['aaaaaa.jpg', 'zaaaaa.jpg'], ['aaaaab.jpg']])

    def test_filtered_segments_do_not_count_and_large_files_stay_whole(self):
        batches = self.generate([('aaaaaa', 'poster', 1, 3, 4),
                                 ('bbbbbb', 'poster', 1, 2, 2),
                                 ('cccccc', 'poster', 1, 5, 5)])
        self.assertEqual(self.names(batches), [['bbbbbb.jpg', 'cccccc.jpg']])
        self.assertEqual(self.output_names, ['cccccc_test.nzb'])

    def test_one_disables_batching(self):
        batches = self.generate([(name, 'poster', 1, 2, 2)
                                 for name in ['aaaaaa', 'abbbbb', 'zaaaaa']], minimum=1)
        self.assertEqual(len(batches), 3)


if __name__ == '__main__':
    unittest.main()
