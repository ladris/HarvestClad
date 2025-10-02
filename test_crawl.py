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
        # Path depth of 21 should be a trap (new default max is 20)
        deep_url = "http://example.com/" + "/".join([f"segment{i}" for i in range(21)])
        self.assertTrue(self.detector.is_trap(deep_url))

        # Path depth of 20 should not be a trap
        ok_url = "http://example.com/" + "/".join([f"segment{i}" for i in range(20)])
        self.assertFalse(self.detector.is_trap(ok_url))

    def test_is_trap_repeating_segments(self):
        # 6 repeating segments 'a' should be a trap (new default max is 5)
        repeat_url = "http://example.com/a/b/a/c/a/d/a/e/a/f/a"
        self.assertTrue(self.detector.is_trap(repeat_url))

        # 5 repeating segments should be fine
        ok_url = "http://example.com/a/b/a/c/a/d/a/e/a"
        self.assertFalse(self.detector.is_trap(ok_url))

    def test_is_trap_query_variations(self):
        # This test uses a custom detector, so it's not affected by the default change,
        # but it remains a valid test for the logic itself.
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

    def test_add_duplicate_links(self):
        """
        Tests that duplicate links (same source and target) can be added
        since the uniqueness constraint has been removed.
        """
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

        # Check that two links were inserted
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(id) FROM links WHERE source_page_id=?", (page_id,))
        count = cursor.fetchone()[0]
        conn.close()
        self.assertEqual(count, 2)

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

        self.db_manager = DatabaseManager(db_path=self.db_path)

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
        self.db_manager.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    @patch('crawl.WebCrawler.crawl_page')
    async def test_worker_handles_links_correctly(self, mock_crawl_page):
        # Events for two-way synchronization
        internal_link_queued = asyncio.Event()
        test_can_continue = asyncio.Event()

        # This side effect simulates the behavior of crawl_page. It's a sync function.
        def crawl_side_effect(url, page_id, depth):
            if url == "http://example.com":
                # Simulate finding one external link (added to DB) and one internal link (returned).
                self.db_manager.add_page("http://another.com/external", "http://another.com/external", parent_url=None, depth=0)
                return ({'status_code': 200}, [
                    ("http://example.com/internal", "http://example.com/internal", "http://example.com", 1)
                ])
            return ({'status_code': 200}, [])

        mock_crawl_page.side_effect = crawl_side_effect

        # Setup manager and initial database state
        start_url = "http://example.com"
        self.db_manager.add_page(start_url, start_url, depth=0)
        manager = CrawlerManager(self.db_manager, self.args)
        manager.crawler = WebCrawler(db_manager=self.db_manager, domain_to_crawl="example.com", max_depth=2)
        manager.domain_to_crawl = "example.com"

        # Wrap the queue's put method to establish a synchronization point.
        original_put = manager.queue.put
        async def put_wrapper(item):
            await original_put(item)
            if item[1] == "http://example.com/internal":
                internal_link_queued.set()  # Signal to the test that the item is queued.
                await test_can_continue.wait()  # Wait for the test to finish its assertions.

        manager.queue.put = put_wrapper

        # Start the crawl by adding the first page and creating the worker.
        await manager.queue.put((1, start_url, 0))
        worker_task = asyncio.create_task(manager.worker("test-worker"))

        # Wait until the worker signals that the internal link is in the queue.
        await asyncio.wait_for(internal_link_queued.wait(), timeout=2)

        # At this point, the worker is paused inside our put_wrapper, waiting for test_can_continue.
        # We can now safely assert the state of the system.

        # Assertions
        self.assertEqual(manager.queue.qsize(), 1, "Internal link should be in the queue")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT url, crawl_depth FROM pages WHERE domain=?", ("another.com",))
        external_page = cursor.fetchone()
        conn.close()
        self.assertIsNotNone(external_page, "External page should be in the database")
        self.assertEqual(external_page[0], "http://another.com/external")
        self.assertEqual(external_page[1], 0, "External page should have depth 0")

        # Cleanup: allow the worker to proceed and then cancel it.
        test_can_continue.set()
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)

    @patch('requests.Session.get')
    async def test_integration_add_link(self, mock_get):
        """
        Integration test to ensure that the 'links' table is populated
        during a real crawl sequence, mocking only the HTTP request.
        """
        # 1. Setup mock HTTP response
        start_url = "http://example.com"
        linked_url = "http://example.com/linked_page"
        html_content = f'<html><body><a href="{linked_url}">A Link</a></body></html>'

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = html_content.encode('utf-8')
        mock_response.headers = {'Content-Type': 'text/html'}
        mock_response.encoding = 'utf-8'
        mock_response.history = []
        mock_get.return_value = mock_response

        # 2. Setup CrawlerManager and initial state
        self.args.continue_crawl = True
        self.args.target_domain = "example.com"
        manager = CrawlerManager(self.db_manager, self.args)

        # The manager.run() method creates its own crawler, so we must ensure
        # it's configured correctly. We can do this by setting up the initial
        # database state that manager.setup_crawler() will use.
        normalized_start_url = manager.db.url_hash(start_url) # Simple normalization for test
        page_id = self.db_manager.add_page(start_url, start_url, depth=0)


        # 3. Run the manager
        # The manager will fetch the uncrawled page, call the (mocked) network request,
        # process the HTML, and should add the discovered link to the DB.
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
        # This HTML has a link with a 'rel' attribute that BeautifulSoup will parse into a list
        html_content = f'<html><body><a href="{linked_url}" rel="noopener nofollow">A Link</a></body></html>'

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = html_content.encode('utf-8')
        mock_response.headers = {'Content-Type': 'text/html'}
        mock_response.encoding = 'utf-8'
        mock_response.history = []
        mock_get.return_value = mock_response

        # 2. Setup CrawlerManager and initial state
        self.args.continue_crawl = True
        self.args.target_domain = "example.com"
        manager = CrawlerManager(self.db_manager, self.args)

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
        # is_follow should be False (represented as 0 in SQLite) because 'nofollow' is present
        self.assertEqual(result[2], 0)

    @patch('builtins.input', return_value='y')
    async def test_idle_monitor_shutdown(self, mock_input):
        """
        Tests that the idle_monitor correctly identifies an idle state
        and triggers the shutdown event after user confirmation.
        """
        # 1. Setup CrawlerManager with a very short timeout
        self.args.idle_timeout = 1
        manager = CrawlerManager(self.db_manager, self.args)

        # 2. Run the idle monitor as a task
        monitor_task = asyncio.create_task(manager.idle_monitor())

        # 3. Wait for slightly longer than the timeout
        await asyncio.sleep(1.5)

        # 4. Assert that the shutdown event was set
        self.assertTrue(manager.shutdown_event.is_set(), "Shutdown event should be set after idle timeout and user confirmation.")

        # 5. Assert that the user was prompted
        mock_input.assert_called_once_with("Crawler is idle. Exit? (y/n): ")

        # 6. Cleanup
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)


if __name__ == '__main__':
    unittest.main()