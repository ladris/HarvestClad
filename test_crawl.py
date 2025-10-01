import unittest
import os
import sqlite3
from unittest.mock import MagicMock, patch
from bs4 import BeautifulSoup
from crawl import DatabaseManager, ResourceExtractor, WebCrawler

class TestResourceExtractor(unittest.TestCase):
    def test_extract_images(self):
        html = """
        <html>
            <body>
                <img src="image1.jpg" alt="Alt text 1">
                <img src="/image2.png">
                <div style="background-image: url('bg.gif');"></div>
                <picture>
                    <source srcset="image3.webp" type="image/webp">
                    <img src="image3.jpg" alt="Alt text 3">
                </picture>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        extractor = ResourceExtractor('http://example.com')
        resources = extractor.extract_images(soup)
        self.assertEqual(len(resources), 5)
        self.assertEqual(resources[0]['url'], 'http://example.com/image1.jpg')
        self.assertEqual(resources[0]['alt_text'], 'Alt text 1')
        self.assertEqual(resources[1]['url'], 'http://example.com/image2.png')
        self.assertEqual(resources[2]['url'], 'http://example.com/image3.jpg')
        self.assertEqual(resources[3]['url'], 'http://example.com/image3.webp')
        self.assertEqual(resources[4]['url'], 'http://example.com/bg.gif')

    def test_extract_documents(self):
        html = """
        <html>
            <body>
                <a href="document.pdf">Download PDF</a>
                <a href="/docs/report.docx">Download Word Doc</a>
            </body>
        </html>
        """
        soup = BeautifulSoup(html, 'html.parser')
        extractor = ResourceExtractor('http://example.com')
        resources = extractor.extract_documents(soup)
        self.assertEqual(len(resources), 2)
        self.assertEqual(resources[0]['url'], 'http://example.com/document.pdf')
        self.assertEqual(resources[1]['url'], 'http://example.com/docs/report.docx')


class TestDatabaseManager(unittest.TestCase):
    def setUp(self):
        self.db_path = "test_crawler.db"
        self.db_manager = DatabaseManager(db_path=self.db_path)

    def tearDown(self):
        self.db_manager.close()
        os.remove(self.db_path)

    def test_add_resource(self):
        self.db_manager.add_page("http://example.com")
        resource_data = {
            'url': 'http://example.com/image.jpg',
            'type': 'image',
            'source_tag': 'img',
            'source_attribute': 'src',
            'alt_text': 'An example image',
            'media_keywords': 'image, jpg'
        }
        self.db_manager.add_resource(1, resource_data)

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM resources")
        row = cursor.fetchone()
        conn.close()

        self.assertIsNotNone(row)
        self.assertEqual(row[2], 'http://example.com/image.jpg')
        self.assertEqual(row[3], 'image')
        self.assertEqual(row[8], 'img')
        self.assertEqual(row[9], 'src')
        self.assertEqual(row[10], 'An example image')
        self.assertEqual(row[11], 'image, jpg')

if __name__ == '__main__':
    unittest.main()