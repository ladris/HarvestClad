import unittest
import os
import sqlite3
import asyncio
import argparse
import threading
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse
from crawl import DatabaseManager, ResourceExtractor, LinkDetector, UrlTrapDetector, WebCrawler, CrawlerManager

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
        # In the new architecture, DatabaseManager requires a ResourceManager.
        from crawl import ResourceManager
        self.resource_manager = ResourceManager(db_path=self.db_path, use_selenium=False, user_agent="test-agent")
        self.db_manager = DatabaseManager(resource_manager=self.resource_manager)
        self.db_manager.init_database() # Manually init for testing
        self.link_detector = LinkDetector(base_url="http://example.com")

    def tearDown(self):
        # Clean up thread-local resources that might have been created.
        self.resource_manager.cleanup_thread_resources()
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

    def test_add_link_uniqueness(self):
        # Add a source page
        page_id = self.db_manager.add_page("http://example.com/source", "http://example.com/source")

        link_data = {
            'target_url': 'http://example.com/target',
            'text': 'Target Link'
        }

        # Add the link for the first time
        self.db_manager.add_link(page_id, link_data)

        # Try to add the exact same link again
        self.db_manager.add_link(page_id, link_data)

        # Check that only one link was inserted
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(id) FROM links WHERE source_page_id=?", (page_id,))
        count = cursor.fetchone()[0]
        conn.close()
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

from bs4 import BeautifulSoup
class TestResourceExtractor(unittest.TestCase):
    def setUp(self):
        self.base_url = "http://example.com"
        self.extractor = ResourceExtractor(base_url=self.base_url)
        self.sample_html = """
        <html>
            <head>
                <link rel="stylesheet" href="/style.css">
                <link rel="icon" href="favicon.ico">
                <script src="script.js"></script>
            </head>
            <body>
                <img src="image.jpg" alt="test image">
                <div style="background-image: url('bg.png');"></div>
                <picture>
                    <source srcset="image.webp" type="image/webp">
                    <img src="image2.jpg">
                </picture>

                <video src="video.mp4"></video>
                <audio>
                    <source src="audio.mp3" type="audio/mpeg">
                </audio>

                <a href="document.pdf">Download PDF</a>
                <a href="/archive.zip">Download ZIP</a>

                <iframe src="embed.html"></iframe>
                <embed src="flash.swf">
                <object data="object.svg"></object>
            </body>
        </html>
        """
        self.soup = BeautifulSoup(self.sample_html, 'html.parser')

    def test_extract_images(self):
        images = self.extractor.extract_images(self.soup)
        urls = {img['url'] for img in images}
        self.assertEqual(len(images), 4)
        self.assertIn("http://example.com/image.jpg", urls)
        self.assertIn("http://example.com/bg.png", urls)
        self.assertIn("http://example.com/image.webp", urls)
        self.assertIn("http://example.com/image2.jpg", urls)

    def test_extract_videos(self):
        videos = self.extractor.extract_videos(self.soup)
        self.assertEqual(len(videos), 1)
        self.assertEqual(videos[0]['url'], "http://example.com/video.mp4")

    def test_extract_audios(self):
        audios = self.extractor.extract_audios(self.soup)
        self.assertEqual(len(audios), 1)
        self.assertEqual(audios[0]['url'], "http://example.com/audio.mp3")

    def test_extract_documents(self):
        docs = self.extractor.extract_documents(self.soup)
        urls = {doc['url'] for doc in docs}
        self.assertEqual(len(docs), 2)
        self.assertIn("http://example.com/document.pdf", urls)
        self.assertIn("http://example.com/archive.zip", urls)

    def test_extract_scripts(self):
        scripts = self.extractor.extract_scripts(self.soup)
        self.assertEqual(len(scripts), 1)
        self.assertEqual(scripts[0]['url'], "http://example.com/script.js")

    def test_extract_stylesheets(self):
        styles = self.extractor.extract_stylesheets(self.soup)
        self.assertEqual(len(styles), 1)
        self.assertEqual(styles[0]['url'], "http://example.com/style.css")

    def test_extract_favicons(self):
        favicons = self.extractor.extract_favicons(self.soup)
        self.assertEqual(len(favicons), 1)
        self.assertEqual(favicons[0]['url'], "http://example.com/favicon.ico")

    def test_extract_embedded_content(self):
        embedded = self.extractor.extract_embedded_content(self.soup)
        urls = {item['url'] for item in embedded}
        self.assertEqual(len(embedded), 3)
        self.assertIn("http://example.com/embed.html", urls)
        self.assertIn("http://example.com/flash.swf", urls)
        self.assertIn("http://example.com/object.svg", urls)

    def test_extract_all_resources(self):
        resources = self.extractor.extract_all_resources(self.soup)
        # 4 images + 1 video + 1 audio + 2 docs + 1 script + 1 style + 1 favicon + 3 embedded = 14
        self.assertEqual(len(resources), 14)

class TestCrawlerManager(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db_path = "test_manager.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

        from crawl import ResourceManager
        self.resource_manager = ResourceManager(self.db_path, use_selenium=False, user_agent="test-agent")
        self.db_manager = DatabaseManager(self.resource_manager)
        self.db_manager.init_database()

        # Mock command-line arguments
        self.args = argparse.Namespace(
            new_scan=None,
            update=False,
            continue_crawl=True,
            target_domain=None,
            max_depth=2,
            delay=0,
            workers=1,
            use_selenium=False,
            disregard_robots=True,
        )

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    @patch('requests.Session.get')
    async def test_integration_add_link(self, mock_get):
        """
        Integration test to ensure that the 'links' table is populated
        during a real crawl sequence, mocking only the HTTP request.
        """
        # 1. Setup mock HTTP response
        start_url = "http://example.com"
        linked_url = "http://example.com/linked_page"
        html_content_start = f'<html><body><a href="{linked_url}">A Link</a></body></html>'
        html_content_linked = '<html><body>No links here</body></html>'

        def mock_get_side_effect(url, **kwargs):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {'Content-Type': 'text/html'}
            mock_response.encoding = 'utf-8'
            mock_response.history = []
            if url == start_url:
                mock_response.content = html_content_start.encode('utf-8')
            else:
                mock_response.content = html_content_linked.encode('utf-8')
            return mock_response

        mock_get.side_effect = mock_get_side_effect

        # 2. Setup CrawlerManager and initial state
        self.args.continue_crawl = True
        self.args.target_domain = "example.com"
        manager = CrawlerManager(self.resource_manager, self.db_manager, self.args)

        page_id = self.db_manager.add_page(start_url, start_url, depth=0)

        # 3. Run the manager
        await manager.run()

        # 4. Assert the link was added to the database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT target_url FROM links WHERE source_page_id=?", (page_id,))
        result = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(result, "A link should have been inserted into the 'links' table.")
        self.assertEqual(result[0], linked_url)

    @patch('requests.Session.get')
    async def test_add_link_with_rel_list(self, mock_get):
        """
        Tests that links are added correctly when the 'rel' attribute
        is a list of strings, which can cause issues with some DB drivers.
        """
        # 1. Setup mock HTTP response
        start_url = "http://example.com/rel-test"
        linked_url = "http://example.com/linked_page"
        html_content_start = f'<html><body><a href="{linked_url}" rel="noopener nofollow">A Link</a></body></html>'
        html_content_linked = '<html><body>No links here</body></html>'

        def mock_get_side_effect(url, **kwargs):
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.headers = {'Content-Type': 'text/html'}
            mock_response.encoding = 'utf-8'
            mock_response.history = []
            if url == start_url:
                mock_response.content = html_content_start.encode('utf-8')
            else:
                mock_response.content = html_content_linked.encode('utf-8')
            return mock_response

        mock_get.side_effect = mock_get_side_effect

        # 2. Setup CrawlerManager and initial state
        self.args.continue_crawl = True
        self.args.target_domain = "example.com"
        manager = CrawlerManager(self.resource_manager, self.db_manager, self.args)

        page_id = self.db_manager.add_page(start_url, start_url, depth=0)

        # 3. Run the manager
        await manager.run()

        # 4. Assert the link was added and the 'rel' attribute was handled
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT target_url, link_rel, is_follow FROM links WHERE source_page_id=?", (page_id,))
        result = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(result, "Link should have been inserted even with a list 'rel' attribute.")
        self.assertEqual(result[0], linked_url)
        self.assertEqual(result[1], "noopener nofollow")
        self.assertEqual(result[2], 0)


if __name__ == '__main__':
    unittest.main()