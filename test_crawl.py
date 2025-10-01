import unittest
import os
import sqlite3
from urllib.parse import urlparse
from crawl import DatabaseManager, ResourceExtractor, LinkDetector, UrlTrapDetector

class TestLinkDetector(unittest.TestCase):
    def setUp(self):
        self.detector = LinkDetector(base_url="http://example.com")

    def test_normalize_url_advanced(self):
        # Test case 1: Sorting query parameters
        url1 = "http://example.com/page?b=2&a=1"
        self.assertEqual(self.detector.normalize_url_advanced(url1, url1), "http://example.com/page?a=1&b=2")

        # Test case 2: Removing tracking parameters
        url2 = "http://example.com?utm_source=google&id=123"
        self.assertEqual(self.detector.normalize_url_advanced(url2, url2), "http://example.com/?id=123")

        # Test case 3: Removing fragments
        url3 = "http://example.com/page.html#section"
        self.assertEqual(self.detector.normalize_url_advanced(url3, url3), "http://example.com/page.html")

        # Test case 4: Lowercasing scheme and netloc
        url4 = "HTTP://Example.COM/Path"
        self.assertEqual(self.detector.normalize_url_advanced(url4, url4), "http://example.com/Path")

        # Test case 5: Removing default ports
        url5_http = "http://example.com:80/path"
        url5_https = "https://example.com:443/path"
        self.assertEqual(self.detector.normalize_url_advanced(url5_http, url5_http), "http://example.com/path")
        self.assertEqual(self.detector.normalize_url_advanced(url5_https, url5_https), "https://example.com/path")

        # Test case 6: Combination
        url6 = "HTTPS://WWW.Example.COM:443/path?c=3&b=2&utm_campaign=test#header"
        self.assertEqual(self.detector.normalize_url_advanced(url6, url6), "https://www.example.com/path?b=2&c=3")

class TestUrlTrapDetector(unittest.TestCase):
    def setUp(self):
        # Use default parameters for the detector in most tests
        self.detector = UrlTrapDetector()

    def test_is_trap_path_depth(self):
        # Path depth of 11 should be a trap (default max is 10)
        deep_url = "http://example.com/" + "/".join([f"segment{i}" for i in range(11)])
        self.assertTrue(self.detector.is_trap(deep_url))

        # Path depth of 10 should not be a trap
        ok_url = "http://example.com/" + "/".join([f"segment{i}" for i in range(10)])
        self.assertFalse(self.detector.is_trap(ok_url))

    def test_is_trap_repeating_segments(self):
        # 4 repeating segments 'a' should be a trap (default max is 3)
        repeat_url = "http://example.com/a/b/a/c/a/d/a"
        self.assertTrue(self.detector.is_trap(repeat_url))

        # 3 repeating segments should be fine
        ok_url = "http://example.com/a/b/a/c/a"
        self.assertFalse(self.detector.is_trap(ok_url))

    def test_is_trap_query_variations(self):
        detector = UrlTrapDetector(max_query_variations=3)
        base_path = "http://example.com/page"

        # These should not be traps
        self.assertFalse(detector.is_trap(f"{base_path}?a=1"))
        self.assertFalse(detector.is_trap(f"{base_path}?b=2"))
        self.assertFalse(detector.is_trap(f"{base_path}?c=3"))

        # This should be the 4th unique query structure, so it's a trap
        self.assertTrue(detector.is_trap(f"{base_path}?d=4"))

        # A duplicate structure should not be a trap
        self.assertFalse(detector.is_trap(f"{base_path}?a=5"))


class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_crawler.db"
        # Ensure no old DB file exists
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        self.db_manager = DatabaseManager(db_path=self.db_path)
        self.link_detector = LinkDetector(base_url="http://example.com")

    def tearDown(self):
        self.db_manager.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_add_page_handles_normalization(self):
        url1 = "http://example.com/page?b=2&a=1"
        normalized_url1 = self.link_detector.normalize_url_advanced(url1, url1)

        url2 = "http://example.com/page?a=1&b=2#section"
        normalized_url2 = self.link_detector.normalize_url_advanced(url2, url2)

        # The two URLs should normalize to the same value
        self.assertEqual(normalized_url1, normalized_url2)

        # Add the first page
        id1 = self.db_manager.add_page(url1, normalized_url1)
        self.assertIsNotNone(id1)

        # Try to add the second page, which is a duplicate by normalization
        id2 = self.db_manager.add_page(url2, normalized_url2)

        # It should return the same ID
        self.assertEqual(id1, id2)

        # Check that only one page was actually inserted
        count = self.db_manager.get_total_pages_count()
        self.assertEqual(count, 1)

    def test_add_resource(self):
        url = "http://example.com/resource_page"
        normalized_url = self.link_detector.normalize_url_advanced(url, url)
        page_id = self.db_manager.add_page(url, normalized_url)

        resource_data = {
            'url': 'http://example.com/image.jpg',
            'type': 'image',
            'source_tag': 'img',
            'source_attribute': 'src',
            'alt_text': 'An example image',
            'media_keywords': 'image, jpg'
        }
        self.db_manager.add_resource(page_id, resource_data)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM resources WHERE page_id=?", (page_id,))
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[2], 'http://example.com/image.jpg')
        self.assertEqual(row[3], 'image')

if __name__ == '__main__':
    unittest.main()