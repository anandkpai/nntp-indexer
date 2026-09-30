import unittest
import xml.etree.ElementTree as ET
import sqlite3
import tempfile
from pathlib import Path

from nntp_lib.collection_matching import collection_name, levenshtein, match_collections
from nntp_lib.db import ensure_db, upsert_headers
from nntp_lib.nzb import create_grouped_nzbs_from_db


def row(name, part=1, poster='poster', date='2026-01-01T00:00:00+00:00'):
    return dict(subject=f'"{name}" yEnc ({part}/2)', from_addr=poster,
                date_utc=date, message_id=f'<{name}-{part}@test>',
                bytes=100, artnum=part, refs=None, lines=1, xref=None)


class CollectionMatchingTests(unittest.TestCase):
    def test_distance(self):
        self.assertEqual(levenshtein('kitten', 'sitting'), 3)
        self.assertEqual(levenshtein('', 'abc'), 3)

    def test_archive_and_recovery_names(self):
        names = ['Example.Release.part01.rar', 'example_release.part02.rar',
                 'Example.Release.vol00+01.par2', 'Example.Release.part01.rar.par2']
        self.assertEqual({collection_name(f'"{name}" yEnc (1/2)') for name in names},
                         {'example release'})

    def test_numbers_no_longer_create_boundaries(self):
        rows = [row('Example.Movie.2025.1080p.mkv'), row('Example.Movi.2025.1080p.nfo'),
                row('Example.Movie.2024.1080p.mkv'), row('Example.Movie.2025.2160p.mkv')]
        self.assertEqual(sorted(len(parts) for _, parts in match_collections(rows)), [4])
        self.assertEqual(len(match_collections([row('Example.Show.S01E01.mkv'),
                                               row('Example.Show.S01E02.mkv')])), 1)

    def test_image_sequence_numbers_at_either_edge(self):
        names = ['long-name-adsfas-1.jpg', 'long-name-adsfas-2.jpg',
                 '001 long-name-adsfas.jpg', '0002_long-name-adsfas.PNG',
                 '1234.long-name-adsfas.webp', 'long-name-adsfas_1234.tiff',
                 '001-long-name-adsfas-002.jpeg']
        self.assertEqual({collection_name(name) for name in names}, {'long name adsfas'})
        self.assertEqual(len(match_collections([row(name) for name in names])), 1)

    def test_attached_numbers_from_reported_names(self):
        names = ['emma watson 0ewpdl.jpg'] + [
            f'emma watson 0ewpdl{i}.jpg' for i in range(2, 9)]
        for threshold in [50, 80, 92]:
            with self.subTest(threshold=threshold):
                self.assertEqual(len(match_collections([row(name) for name in names], threshold)), 1)
        # All digit variants now have identical comparison names.
        self.assertEqual(len(match_collections([row(name) for name in names], 100)), 1)

    def test_all_numeric_identifiers_are_removed(self):
        for left, right in [('S01E01', 'S01E02'), ('1x01', '1x02'),
                            ('1080p', '2160p'), ('4k', '8k'), ('2024 event', '2025 event')]:
            with self.subTest(left=left, right=right):
                self.assertEqual(len(match_collections([
                    row(f'Example release {left}.jpg'),
                    row(f'Example release {right}.jpg')], 100)), 1)

    def test_sequence_removal_limits(self):
        for name, expected in [('12345-name.jpg', 'name'),
                               ('name-12345.jpg', 'name'),
                               ('name123.jpg', 'name'),
                               ('123name.jpg', 'name'),
                               ('name-12-event.jpg', 'name event'),
                               ('001.jpg', ''), ('002.jpg', ''),
                               ('001-002.jpg', '')]:
            with self.subTest(name=name):
                self.assertEqual(collection_name(name), expected)

    def test_edge_years_follow_requested_sequence_rule(self):
        self.assertEqual(collection_name('2025-name.jpg'), 'name')
        self.assertEqual(collection_name('name-2025.jpg'), 'name')

    def test_poster_time_and_short_names(self):
        self.assertEqual(len(match_collections([row('Example.Movie.rar'),
                                               row('Example.Movie.nfo', poster='other')])), 2)
        self.assertEqual(len(match_collections([row('Example.Movie.rar'),
                         row('Example.Movie.nfo', date='2026-01-10T00:00:00Z')])), 2)
        self.assertEqual(len(match_collections([row('cat.rar'), row('bat.nfo')], 50)), 1)
        self.assertEqual(len(match_collections([row('001.jpg'), row('002.jpg')], 0)), 2)
        self.assertEqual(len(match_collections([row('Example.Movie.rar', date=None),
                                               row('Example.Movie.nfo')])), 1)

    def test_fixed_anchor_prevents_transitive_merge(self):
        names = ['abcdefghijklmnopqrst', 'abcdefghijklmnopqrsx', 'abcdefghijklmnopqrxx']
        self.assertEqual(len(match_collections([row(name + '.rar') for name in names], 92)), 2)

    def test_configurable_threshold(self):
        rows = [row('abcdefghij.jpg'), row('abcdefghxy.jpg')]
        self.assertEqual(len(match_collections(rows)), 1)
        self.assertEqual(len(match_collections(rows, 80)), 1)
        self.assertEqual(len(match_collections(rows, 81)), 2)
        self.assertEqual(len(match_collections(rows, 92)), 2)
        self.assertEqual(len(match_collections(rows, 100)), 2)
        # The length-based shortcut must use the configured threshold too.
        rows = [row('abcdefghij.jpg'), row('abcdefghijkl.jpg')]
        self.assertEqual(len(match_collections(rows, 80)), 1)
        self.assertEqual(len(match_collections(rows, 92)), 2)
        for threshold in [-1, 101, float('nan')]:
            with self.assertRaises(ValueError):
                match_collections([], threshold)

    def test_nzb_keeps_files_segments_and_small_collections(self):
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / 'test.sqlite')
            rows = [row(name, part) for name in ['Example.Movie.part01.rar',
                    'Example.Movi.part02.rar', 'Unrelated.Release.rar',
                    'long-name-adsfas-1.jpg', 'long-name-adsfas-2.jpg',
                    '001 long-name-adsfas.jpg', '002 long-name-adsfas.jpg',
                    'emma watson 0ewpdl.jpg', 'emma watson 0ewpdl2.jpg',
                    'emma watson 0ewpdl3.jpg'] for part in [1, 2]]
            for i, article in enumerate(rows, 1):
                article['artnum'] = i
            with sqlite3.connect(path) as conn:
                ensure_db(conn)
                upsert_headers(conn, 'test', rows)
            results = create_grouped_nzbs_from_db(path, 'test', directory,
                       require_complete_sets=True, min_articles_per_nzb=1, fuzzy_grouping=True)
        self.assertEqual(len(results), 4)
        ns = {'n': 'http://www.newzbin.com/DTD/2003/nzb'}
        files = [file for _, xml in results for file in ET.fromstring(xml).findall('n:file', ns)]
        self.assertEqual(len(files), 10)
        self.assertEqual(sorted(len(ET.fromstring(xml).findall('n:file', ns))
                                for _, xml in results), [1, 2, 3, 4])
        ids = []
        for file in files:
            segments = file.findall('n:segments/n:segment', ns)
            self.assertEqual([s.get('number') for s in segments], ['1', '2'])
            ids.extend(s.text for s in segments)
        self.assertEqual(set(ids), {r['message_id'][1:-1] for r in rows})


if __name__ == '__main__':
    unittest.main()
