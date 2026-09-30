import sqlite3
import tempfile
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path

from nntp_lib.db import ensure_db, upsert_headers
from nntp_lib.nzb import create_grouped_nzbs_from_db, _batch_filename


class NzbBatchingTests(unittest.TestCase):
    def generate(self, sizes, minimum=20, incomplete=False):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'test.sqlite')
            with sqlite3.connect(path) as conn:
                ensure_db(conn)
                rows = []
                for collection, size in enumerate(sizes):
                    total = size + 1 if incomplete and collection == 0 else size
                    for part in range(1, size + 1):
                        rows.append(dict(
                            message_id=f'<{collection}-{part}@example.test>',
                            artnum=len(rows) + 1,
                            subject=f'Collection{chr(65 + collection)} - "file.bin" yEnc ({part}/{total})',
                            from_addr=f'poster{collection}@example.test',
                            date_utc=None, refs=None, bytes=100, lines=1, xref=None,
                        ))
                upsert_headers(conn, 'alt.binaries.test', rows)
            return create_grouped_nzbs_from_db(
                path, 'alt.binaries.test', directory,
                require_complete_sets=True, min_articles_per_nzb=minimum,
            )

    def counts(self, results):
        ns = {'n': 'http://www.newzbin.com/DTD/2003/nzb'}
        return [len(ET.fromstring(xml).findall('.//n:segment', ns))
                for _, xml in results]

    def test_accumulates_collections_and_preserves_final_remainder(self):
        results = self.generate([8, 7, 6, 4])
        self.assertEqual(self.counts(results), [21, 4])
        ns = {'n': 'http://www.newzbin.com/DTD/2003/nzb'}
        files = ET.fromstring(results[0][1]).findall('n:file', ns)
        self.assertEqual(len(files), 3)
        for file, size in zip(files, [8, 7, 6]):
            segments = file.findall('n:segments/n:segment', ns)
            self.assertEqual([int(s.get('number')) for s in segments],
                             list(range(1, size + 1)))
        ids = [s.text for _, xml in results
               for s in ET.fromstring(xml).findall('.//n:segment', ns)]
        self.assertEqual(len(set(ids)), 25)

    def test_exact_threshold_and_large_collections_stay_intact(self):
        self.assertEqual(self.counts(self.generate([20, 25])), [20, 25])

    def test_batch_names_describe_headers_and_remainder(self):
        results = self.generate([8, 7, 6, 4])
        self.assertEqual(results[0][0],
                         'CollectionA_alt_binaries_test.nzb')
        self.assertEqual(results[1][0],
                         'CollectionD_alt_binaries_test.nzb')

    def test_batch_name_prefers_largest_collection(self):
        results = self.generate([2, 18])
        self.assertEqual(results[0][0], 'CollectionB_alt_binaries_test.nzb')

    def test_batch_name_fallback_and_length(self):
        root = ET.Element('nzb')
        ET.SubElement(root, 'file', subject='')
        self.assertEqual(_batch_filename(root, 'alt.binaries.test', set()),
                         'alt_binaries_test.nzb')
        root[0].set('subject', '\u00e9' * 300 + '/unsafe:name')
        used_names = set()
        name = _batch_filename(root, 'group' * 100, used_names)
        self.assertLess(len(name.encode('utf-8')), 255)
        self.assertNotIn('/', name)
        self.assertNotIn(':', name)
        self.assertEqual(_batch_filename(root, 'group' * 100, used_names), name[:-4] + '_2.nzb')

    def test_disabled_threshold_keeps_collection_names(self):
        results = self.generate([8, 7], minimum=1)
        self.assertEqual(self.counts(results), [8, 7])
        self.assertTrue(all('_batch_' not in name for name, _ in results))

    def test_excluded_incomplete_segments_do_not_count(self):
        self.assertEqual(self.counts(self.generate([19, 8, 12], incomplete=True)), [20])

    def test_empty_results(self):
        self.assertEqual(self.generate([]), [])

    def test_invalid_threshold(self):
        with self.assertRaises(ValueError):
            self.generate([1], minimum=0)


if __name__ == '__main__':
    unittest.main()
