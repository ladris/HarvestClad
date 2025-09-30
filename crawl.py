#!/usr/bin/env python3
"""
Advanced Web Crawler with Deep Link Analysis
Detects all types of links including dynamic/JavaScript-rendered content
Stores comprehensive metadata in SQLite database
"""

import argparse
import sqlite3
import requests
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
from urllib.robotparser import RobotFileParser
from bs4 import BeautifulSoup
import re
import xml.etree.ElementTree as ET
import json
import hashlib
from datetime import datetime
from collections import deque
import time
import logging
from typing import Set, Dict, List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages SQLite database operations"""
    
    def __init__(self, db_path: str = "crawler_data.db"):
        self.db_path = db_path
        self.connection = None
        self.init_database()
    
    def init_database(self):
        """Initialize database schema"""
        self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
        cursor = self.connection.cursor()
        
        # Pages table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                url_hash TEXT UNIQUE NOT NULL,
                normalized_url TEXT,
                domain TEXT,
                scheme TEXT,
                path TEXT,
                query_string TEXT,
                fragment TEXT,
                discovered_at TIMESTAMP,
                first_crawled_at TIMESTAMP,
                last_crawled_at TIMESTAMP,
                crawl_count INTEGER DEFAULT 0,
                status_code INTEGER,
                response_time_ms INTEGER,
                content_type TEXT,
                content_length INTEGER,
                title TEXT,
                meta_description TEXT,
                meta_keywords TEXT,
                canonical_url TEXT,
                robots_meta TEXT,
                og_title TEXT,
                og_description TEXT,
                og_image TEXT,
                og_type TEXT,
                twitter_card TEXT,
                language TEXT,
                encoding TEXT,
                is_crawled BOOLEAN DEFAULT 0,
                crawl_depth INTEGER DEFAULT 0,
                parent_url TEXT,
                error_message TEXT,
                redirect_url TEXT,
                redirect_chain TEXT
            )
        """)
        
        # Links table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_page_id INTEGER,
                target_url TEXT NOT NULL,
                target_url_hash TEXT NOT NULL,
                link_text TEXT,
                link_title TEXT,
                link_type TEXT,
                link_rel TEXT,
                is_internal BOOLEAN,
                is_follow BOOLEAN,
                is_external BOOLEAN,
                xpath TEXT,
                css_selector TEXT,
                position_index INTEGER,
                detected_method TEXT,
                is_javascript BOOLEAN DEFAULT 0,
                is_dynamic BOOLEAN DEFAULT 0,
                onclick_handler TEXT,
                href_attribute TEXT,
                data_attributes TEXT,
                aria_label TEXT,
                surrounding_text TEXT,
                link_context TEXT,
                discovered_at TIMESTAMP,
                FOREIGN KEY (source_page_id) REFERENCES pages(id),
                UNIQUE(source_page_id, target_url_hash, position_index)
            )
        """)
        
        # JavaScript events table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS javascript_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_id INTEGER,
                event_type TEXT,
                element_tag TEXT,
                element_id TEXT,
                element_class TEXT,
                handler_code TEXT,
                detected_url TEXT,
                discovered_at TIMESTAMP,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            )
        """)
        
        # Resources table (CSS, JS, images, etc.)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS resources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                page_id INTEGER,
                resource_url TEXT NOT NULL,
                resource_type TEXT,
                size_bytes INTEGER,
                load_time_ms INTEGER,
                mime_type TEXT,
                discovered_at TIMESTAMP,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_url_hash ON pages(url_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_is_crawled ON pages(is_crawled)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain ON pages(domain)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_target_hash ON links(target_url_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_link_type ON links(link_type)")
        
        self.connection.commit()
        logger.info(f"Database initialized: {self.db_path}")
    
    def url_hash(self, url: str) -> str:
        """Generate hash for URL"""
        return hashlib.sha256(url.encode()).hexdigest()
    
    def add_page(self, url: str, parent_url: str = None, depth: int = 0) -> int:
        """Add page to database if not exists"""
        cursor = self.connection.cursor()
        url_hash = self.url_hash(url)
        parsed = urlparse(url)
        
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO pages 
                (url, url_hash, normalized_url, domain, scheme, path, query_string, 
                 fragment, discovered_at, crawl_depth, parent_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                url, url_hash, url.split('#')[0], parsed.netloc, parsed.scheme,
                parsed.path, parsed.query, parsed.fragment, datetime.now(),
                depth, parent_url
            ))
            self.connection.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            cursor.execute("SELECT id FROM pages WHERE url_hash = ?", (url_hash,))
            result = cursor.fetchone()
            return result[0] if result else None
    
    def update_page_crawl(self, page_id: int, data: Dict):
        """Update page after crawling"""
        cursor = self.connection.cursor()
        now = datetime.now()
        
        cursor.execute("""
            UPDATE pages SET
                first_crawled_at = COALESCE(first_crawled_at, ?),
                last_crawled_at = ?,
                crawl_count = crawl_count + 1,
                is_crawled = 1,
                status_code = ?,
                response_time_ms = ?,
                content_type = ?,
                content_length = ?,
                title = ?,
                meta_description = ?,
                meta_keywords = ?,
                canonical_url = ?,
                robots_meta = ?,
                og_title = ?,
                og_description = ?,
                og_image = ?,
                og_type = ?,
                twitter_card = ?,
                language = ?,
                encoding = ?,
                error_message = ?,
                redirect_url = ?,
                redirect_chain = ?
            WHERE id = ?
        """, (
            now, now,
            data.get('status_code'),
            data.get('response_time_ms'),
            data.get('content_type'),
            data.get('content_length'),
            data.get('title'),
            data.get('meta_description'),
            data.get('meta_keywords'),
            data.get('canonical_url'),
            data.get('robots_meta'),
            data.get('og_title'),
            data.get('og_description'),
            data.get('og_image'),
            data.get('og_type'),
            data.get('twitter_card'),
            data.get('language'),
            data.get('encoding'),
            data.get('error_message'),
            data.get('redirect_url'),
            data.get('redirect_chain'),
            page_id
        ))
        self.connection.commit()
    
    def add_link(self, source_page_id: int, link_data: Dict):
        """Add link to database"""
        cursor = self.connection.cursor()
        target_hash = self.url_hash(link_data['target_url'])
        
        try:
            cursor.execute("""
                INSERT OR IGNORE INTO links 
                (source_page_id, target_url, target_url_hash, link_text, link_title,
                 link_type, link_rel, is_internal, is_follow, is_external,
                 xpath, css_selector, position_index, detected_method,
                 is_javascript, is_dynamic, onclick_handler, href_attribute,
                 data_attributes, aria_label, surrounding_text, link_context,
                 discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                source_page_id, link_data['target_url'], target_hash,
                link_data.get('text'), link_data.get('title'),
                link_data.get('type'), link_data.get('rel'),
                link_data.get('is_internal'), link_data.get('is_follow'),
                link_data.get('is_external'), link_data.get('xpath'),
                link_data.get('css_selector'), link_data.get('position'),
                link_data.get('detected_method'), link_data.get('is_javascript'),
                link_data.get('is_dynamic'), link_data.get('onclick'),
                link_data.get('href'), link_data.get('data_attributes'),
                link_data.get('aria_label'), link_data.get('surrounding_text'),
                link_data.get('context'), datetime.now()
            ))
            self.connection.commit()
        except sqlite3.IntegrityError:
            pass  # Duplicate link
    
    def add_javascript_event(self, page_id: int, event_data: Dict):
        """Add JavaScript event to database"""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO javascript_events 
            (page_id, event_type, element_tag, element_id, element_class,
             handler_code, detected_url, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            page_id, event_data.get('type'), event_data.get('tag'),
            event_data.get('id'), event_data.get('class'),
            event_data.get('handler'), event_data.get('url'),
            datetime.now()
        ))
        self.connection.commit()
    
    def add_resource(self, page_id: int, resource_data: Dict):
        """Add resource to database"""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO resources 
            (page_id, resource_url, resource_type, size_bytes, load_time_ms,
             mime_type, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            page_id, resource_data.get('url'), resource_data.get('type'),
            resource_data.get('size'), resource_data.get('load_time'),
            resource_data.get('mime_type'), datetime.now()
        ))
        self.connection.commit()
    
    def get_next_uncrawled(self) -> Optional[tuple]:
        """Get next uncrawled page"""
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT id, url, crawl_depth 
            FROM pages 
            WHERE is_crawled = 0 
            ORDER BY crawl_depth ASC, discovered_at ASC 
            LIMIT 1
        """)
        return cursor.fetchone()

    def get_distinct_domains(self) -> List[str]:
        """Get a list of distinct domains from the pages table"""
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT domain FROM pages WHERE domain IS NOT NULL ORDER BY domain")
        return [row[0] for row in cursor.fetchall()]

    def reset_domain_crawl_status(self, domain: str):
        """Reset the crawl status for a specific domain."""
        cursor = self.connection.cursor()
        cursor.execute("UPDATE pages SET is_crawled = 0 WHERE domain = ?", (domain,))
        self.connection.commit()
        logger.info(f"Crawl status reset for domain: {domain}")
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()


class LinkDetector:
    """Detects all types of links from HTML and JavaScript"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
    
    def is_internal(self, url: str) -> bool:
        """Check if URL is internal"""
        parsed = urlparse(url)
        return parsed.netloc == self.base_domain or parsed.netloc == ''
    
    def normalize_url(self, url: str, current_url: str) -> Optional[str]:
        """Normalize and resolve URL"""
        if not url or url.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
            return None
        
        # Handle relative URLs
        absolute_url = urljoin(current_url, url)
        
        # Parse and clean
        parsed = urlparse(absolute_url)
        
        # Remove fragments for uniqueness (optional)
        # normalized = urlunparse((parsed.scheme, parsed.netloc, parsed.path, 
        #                         parsed.params, parsed.query, ''))
        
        return absolute_url
    
    def extract_static_links(self, soup: BeautifulSoup, current_url: str) -> List[Dict]:
        """Extract static HTML links"""
        links = []
        position = 0
        
        # <a> tags
        for idx, tag in enumerate(soup.find_all('a', href=True)):
            url = self.normalize_url(tag['href'], current_url)
            if url:
                links.append({
                    'target_url': url,
                    'text': tag.get_text(strip=True)[:500],
                    'title': tag.get('title'),
                    'type': 'anchor',
                    'rel': tag.get('rel'),
                    'is_internal': self.is_internal(url),
                    'is_follow': 'nofollow' not in tag.get('rel', []),
                    'is_external': not self.is_internal(url),
                    'position': position,
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('href'),
                    'aria_label': tag.get('aria-label'),
                    'data_attributes': json.dumps({k: v for k, v in tag.attrs.items() if k.startswith('data-')})
                })
                position += 1
        
        # <link> tags
        for tag in soup.find_all('link', href=True):
            url = self.normalize_url(tag['href'], current_url)
            if url:
                links.append({
                    'target_url': url,
                    'type': 'link_tag',
                    'rel': tag.get('rel'),
                    'is_internal': self.is_internal(url),
                    'is_follow': True,
                    'is_external': not self.is_internal(url),
                    'position': position,
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('href')
                })
                position += 1
        
        # <form> actions
        for tag in soup.find_all('form', action=True):
            url = self.normalize_url(tag['action'], current_url)
            if url:
                links.append({
                    'target_url': url,
                    'type': 'form',
                    'is_internal': self.is_internal(url),
                    'is_follow': True,
                    'is_external': not self.is_internal(url),
                    'position': position,
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('action')
                })
                position += 1
        
        # <iframe> src
        for tag in soup.find_all('iframe', src=True):
            url = self.normalize_url(tag['src'], current_url)
            if url:
                links.append({
                    'target_url': url,
                    'type': 'iframe',
                    'is_internal': self.is_internal(url),
                    'is_follow': True,
                    'is_external': not self.is_internal(url),
                    'position': position,
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('src')
                })
                position += 1
        
        # onclick attributes
        for tag in soup.find_all(onclick=True):
            onclick = tag.get('onclick', '')
            urls = self.extract_urls_from_js(onclick, current_url)
            for url in urls:
                links.append({
                    'target_url': url,
                    'text': tag.get_text(strip=True)[:500],
                    'type': 'onclick',
                    'is_internal': self.is_internal(url),
                    'is_follow': True,
                    'is_external': not self.is_internal(url),
                    'position': position,
                    'detected_method': 'onclick_attribute',
                    'is_javascript': True,
                    'is_dynamic': False,
                    'onclick': onclick[:1000]
                })
                position += 1
        
        return links
    
    def extract_urls_from_js(self, js_code: str, current_url: str) -> List[str]:
        """Extract URLs from JavaScript code"""
        urls = []
        
        # Common patterns
        patterns = [
            r'["\']([^"\']*?\.(?:html?|php|aspx?|jsp|cfm)[^"\']*?)["\']',
            r'location\.href\s*=\s*["\']([^"\']+)["\']',
            r'window\.location\s*=\s*["\']([^"\']+)["\']',
            r'window\.open\(["\']([^"\']+)["\']',
            r'(?:fetch|axios\.get)\(["\']([^"\']+)["\']',
            r'["\']([^"\']*?/[^"\']*?)["\']',  # Any path
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, js_code, re.IGNORECASE)
            for match in matches:
                normalized = self.normalize_url(match, current_url)
                if normalized and normalized not in urls:
                    urls.append(normalized)
        
        return urls
    
    def extract_javascript_links(self, soup: BeautifulSoup, current_url: str) -> List[Dict]:
        """Extract links from JavaScript code"""
        links = []
        position = 0
        
        for script in soup.find_all('script'):
            script_content = script.string or ''
            urls = self.extract_urls_from_js(script_content, current_url)
            
            for url in urls:
                links.append({
                    'target_url': url,
                    'type': 'javascript',
                    'is_internal': self.is_internal(url),
                    'is_follow': True,
                    'is_external': not self.is_internal(url),
                    'position': position,
                    'detected_method': 'javascript_code',
                    'is_javascript': True,
                    'is_dynamic': False,
                    'context': script_content[:500] if len(script_content) > 500 else script_content
                })
                position += 1
        
        return links


class WebCrawler:
    """Main crawler class"""
    
    def __init__(self, start_url: str, max_depth: int = 3, 
                 delay: float = 1.0, use_selenium: bool = True, disregard_robots: bool = False):
        self.start_url = start_url
        self.max_depth = max_depth
        self.delay = delay
        self.use_selenium = use_selenium
        self.disregard_robots = disregard_robots
        
        self.db = DatabaseManager()
        self.link_detector = LinkDetector(start_url)
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (AdvancedCrawler/1.0)'
        self.robot_parsers = {}
        
        # Request session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.user_agent
        })
        
        # Selenium setup
        self.driver = None
        if use_selenium:
            self.setup_selenium()

    def parse_sitemap(self, domain: str):
        """Finds, fetches, and parses the sitemap(s) for a domain."""
        robot_parser = self.get_robot_parser(domain)
        sitemap_urls = []
        if robot_parser and robot_parser.sitemaps:
            sitemap_urls.extend(robot_parser.sitemaps)
        else:
            # Fallback to default location
            sitemap_urls.append(urlunparse(('https', domain, '/sitemap.xml', '', '', '')))

        for sitemap_url in sitemap_urls:
            try:
                response = self.session.get(sitemap_url, timeout=15)
                if response.status_code == 200:
                    logger.info(f"Parsing sitemap: {sitemap_url}")
                    self.extract_urls_from_sitemap(response.content)
            except Exception as e:
                logger.warning(f"Could not fetch or parse sitemap {sitemap_url}: {e}")

    def extract_urls_from_sitemap(self, sitemap_content: bytes):
        """Extracts URLs from sitemap XML content."""
        try:
            root = ET.fromstring(sitemap_content)
            # XML namespace is often present and needs to be handled
            namespace = {'ns': root.tag.split('}')[0][1:]} if '}' in root.tag else {'ns': ''}

            # Find all <loc> tags, which contain the URLs
            urls = [
                loc.text.strip()
                for loc in root.findall('.//ns:loc', namespaces=namespace)
            ]

            for url in urls:
                if self.link_detector.is_internal(url):
                    self.db.add_page(url, parent_url='sitemap', depth=0)

            logger.info(f"Added {len(urls)} URLs from sitemap to the queue.")

        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")

    def get_robot_parser(self, domain: str) -> Optional[RobotFileParser]:
        """Fetches, parses, and caches the robots.txt file for a domain."""
        if domain in self.robot_parsers:
            return self.robot_parsers[domain]

        robots_url = urlunparse(('https', domain, '/robots.txt', '', '', ''))
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            parser.read()
            self.robot_parsers[domain] = parser
            logger.info(f"Successfully parsed robots.txt for {domain}")
            return parser
        except Exception as e:
            logger.warning(f"Could not fetch or parse robots.txt for {domain}: {e}")
            self.robot_parsers[domain] = None # Cache failure to avoid retries
            return None
    
    def setup_selenium(self):
        """Setup Selenium WebDriver"""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument(f'--user-agent={self.user_agent}')
            
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Selenium WebDriver initialized")
        except Exception as e:
            logger.warning(f"Could not initialize Selenium: {e}")
            self.use_selenium = False
    
    def crawl_page_static(self, url: str, page_id: int, depth: int) -> Dict:
        """Crawl page using requests"""
        start_time = time.time()
        page_data = {}
        
        try:
            response = self.session.get(url, timeout=30, allow_redirects=True)
            response_time = int((time.time() - start_time) * 1000)
            
            page_data['status_code'] = response.status_code
            page_data['response_time_ms'] = response_time
            page_data['content_type'] = response.headers.get('Content-Type')
            page_data['content_length'] = len(response.content)
            page_data['encoding'] = response.encoding
            
            if response.history:
                page_data['redirect_url'] = response.url
                page_data['redirect_chain'] = json.dumps([r.url for r in response.history])
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Extract metadata
                page_data['title'] = soup.title.string if soup.title else None
                
                meta_desc = soup.find('meta', attrs={'name': 'description'})
                page_data['meta_description'] = meta_desc['content'] if meta_desc else None
                
                meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
                page_data['meta_keywords'] = meta_keywords['content'] if meta_keywords else None
                
                canonical = soup.find('link', attrs={'rel': 'canonical'})
                page_data['canonical_url'] = canonical['href'] if canonical else None
                
                robots = soup.find('meta', attrs={'name': 'robots'})
                page_data['robots_meta'] = robots['content'] if robots else None
                
                # Open Graph
                og_title = soup.find('meta', property='og:title')
                page_data['og_title'] = og_title['content'] if og_title else None
                
                og_desc = soup.find('meta', property='og:description')
                page_data['og_description'] = og_desc['content'] if og_desc else None
                
                og_img = soup.find('meta', property='og:image')
                page_data['og_image'] = og_img['content'] if og_img else None
                
                og_type = soup.find('meta', property='og:type')
                page_data['og_type'] = og_type['content'] if og_type else None
                
                # Twitter Card
                tw_card = soup.find('meta', attrs={'name': 'twitter:card'})
                page_data['twitter_card'] = tw_card['content'] if tw_card else None
                
                lang = soup.find('html')
                page_data['language'] = lang.get('lang') if lang else None
                
                # Extract links
                static_links = self.link_detector.extract_static_links(soup, url)
                js_links = self.link_detector.extract_javascript_links(soup, url)
                
                all_links = static_links + js_links
                
                # Store links
                for link in all_links:
                    self.db.add_link(page_id, link)
                    
                    # Add to crawl queue if internal and within depth
                    if link['is_internal'] and depth < self.max_depth:
                        new_page_id = self.db.add_page(
                            link['target_url'], 
                            parent_url=url, 
                            depth=depth + 1
                        )
                
                logger.info(f"Found {len(all_links)} links on {url}")
        
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            page_data['error_message'] = str(e)
        
        return page_data
    
    def crawl_page_selenium(self, url: str, page_id: int, depth: int) -> Dict:
        """Crawl page using Selenium for dynamic content"""
        page_data = {}
        start_time = time.time()
        
        try:
            self.driver.get(url)
            
            # Wait for page load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Additional wait for JS to execute
            time.sleep(2)
            
            response_time = int((time.time() - start_time) * 1000)
            page_data['response_time_ms'] = response_time
            
            # Get page source after JS execution
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Extract metadata (same as static)
            page_data['title'] = self.driver.title
            page_data['status_code'] = 200  # Selenium doesn't provide status code easily
            
            # Extract links from rendered page
            static_links = self.link_detector.extract_static_links(soup, url)
            js_links = self.link_detector.extract_javascript_links(soup, url)
            
            # Extract dynamic links using Selenium
            dynamic_links = []
            clickable_elements = self.driver.find_elements(By.XPATH, 
                "//*[@onclick or @href or contains(@class, 'link') or contains(@class, 'btn')]")
            
            for idx, element in enumerate(clickable_elements[:100]):  # Limit to avoid overload
                try:
                    href = element.get_attribute('href')
                    onclick = element.get_attribute('onclick')
                    text = element.text[:500]
                    
                    detected_url = None
                    if href:
                        detected_url = self.link_detector.normalize_url(href, url)
                    elif onclick:
                        urls = self.link_detector.extract_urls_from_js(onclick, url)
                        detected_url = urls[0] if urls else None
                    
                    if detected_url:
                        dynamic_links.append({
                            'target_url': detected_url,
                            'text': text,
                            'type': 'dynamic',
                            'is_internal': self.link_detector.is_internal(detected_url),
                            'is_follow': True,
                            'is_external': not self.link_detector.is_internal(detected_url),
                            'position': idx,
                            'detected_method': 'selenium',
                            'is_javascript': bool(onclick),
                            'is_dynamic': True,
                            'onclick': onclick
                        })
                except:
                    continue
            
            all_links = static_links + js_links + dynamic_links
            
            # Store links and queue new pages
            for link in all_links:
                self.db.add_link(page_id, link)
                
                if link['is_internal'] and depth < self.max_depth:
                    self.db.add_page(
                        link['target_url'], 
                        parent_url=url, 
                        depth=depth + 1
                    )
            
            logger.info(f"Found {len(all_links)} links (including {len(dynamic_links)} dynamic) on {url}")
        
        except Exception as e:
            logger.error(f"Error with Selenium on {url}: {e}")
            page_data['error_message'] = str(e)
        
        return page_data
    
    def crawl_page(self, url: str, page_id: int, depth: int):
        """Crawl a single page"""
        logger.info(f"Crawling: {url} (depth: {depth})")
        
        # Try Selenium first for dynamic content, fallback to requests
        if self.use_selenium and self.driver:
            page_data = self.crawl_page_selenium(url, page_id, depth)
        else:
            page_data = self.crawl_page_static(url, page_id, depth)
        
        # Update database
        self.db.update_page_crawl(page_id, page_data)
        
        # Respect crawl delay
        time.sleep(self.delay)
    
    def start(self):
        """Start crawling process"""
        logger.info(f"Starting crawler from: {self.start_url}")
        
        # Add start URL and parse sitemap
        domain = urlparse(self.start_url).netloc
        self.db.add_page(self.start_url, depth=0)
        self.parse_sitemap(domain)
        
        # Crawl loop
        while True:
            next_page = self.db.get_next_uncrawled()
            
            if not next_page:
                logger.info("No more pages to crawl")
                break
            
            page_id, url, depth = next_page

            # Check robots.txt before crawling
            if not self.disregard_robots:
                domain = urlparse(url).netloc
                robot_parser = self.get_robot_parser(domain)
                if robot_parser and not robot_parser.can_fetch(self.user_agent, url):
                    logger.info(f"Skipping (disallowed by robots.txt): {url}")
                    self.db.update_page_crawl(page_id, {
                        'status_code': 403,  # Use a specific code for disallowed
                        'error_message': 'Disallowed by robots.txt'
                    })
                    continue
            
            if depth > self.max_depth:
                logger.info(f"Max depth reached, skipping: {url}")
                continue
            
            self.crawl_page(url, page_id, depth)
        
        logger.info("Crawling complete!")
        self.cleanup()
    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            self.driver.quit()
        self.db.close()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Advanced Web Crawler")
    parser.add_argument("start_url", nargs='?', default=None, help="The URL to start crawling from.")
    parser.add_argument("-d", "--max-depth", type=int, default=3, help="Maximum crawl depth.")
    parser.add_argument("-w", "--delay", type=float, default=1.0, help="Delay between requests in seconds.")
    parser.add_argument("-s", "--use-selenium", action='store_true', help="Use Selenium for dynamic content.")
    parser.add_argument("-u", "--update", action='store_true', help="Update an existing domain from the database.")
    parser.add_argument("--disregard-robots", action='store_true', help="Disregard robots.txt and its rule settings.")
    
    args = parser.parse_args()
    
    start_url = args.start_url

    if args.update:
        db_manager = DatabaseManager()
        try:
            domains = db_manager.get_distinct_domains()
            if not domains:
                print("No domains found in the database to update.")
                return

            print("Please choose a domain to update:")
            for i, domain in enumerate(domains):
                print(f"{i + 1}: {domain}")

            choice_str = input("Enter the number of the domain: ")
            choice = int(choice_str) - 1
            if 0 <= choice < len(domains):
                selected_domain = domains[choice]
                print(f"Preparing to update domain: {selected_domain}")
                db_manager.reset_domain_crawl_status(selected_domain)
                cursor = db_manager.connection.cursor()
                cursor.execute("SELECT url FROM pages WHERE domain = ? ORDER BY discovered_at ASC LIMIT 1", (selected_domain,))
                result = cursor.fetchone()
                if result:
                    start_url = result[0]
                    print(f"Starting update from URL: {start_url}")
                else:
                    print(f"Could not find a starting URL for domain {selected_domain}.")
                    return
            else:
                print("Invalid choice.")
                return
        except (ValueError, IndexError):
            print("Invalid input.")
            return
        finally:
            db_manager.close()

    if not start_url:
        parser.error("A start_url is required, either as an argument or by selecting a domain with --update.")

    crawler = WebCrawler(
        start_url=start_url,
        max_depth=args.max_depth,
        delay=args.delay,
        use_selenium=args.use_selenium,
        disregard_robots=args.disregard_robots
    )

    try:
        crawler.start()
    except Exception as e:
        logger.error(f"An unexpected error occurred during crawling: {e}")
        crawler.cleanup()


if __name__ == "__main__":
    main()
