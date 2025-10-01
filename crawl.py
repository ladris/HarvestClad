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
import os
import platform
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor

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
                normalized_url_hash TEXT UNIQUE,
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
                UNIQUE(source_page_id, target_url_hash)
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
                source_tag TEXT,
                source_attribute TEXT,
                alt_text TEXT,
                media_keywords TEXT,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            )
        """)
        
        # Create indexes
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_url_hash ON pages(url_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_normalized_url_hash ON pages(normalized_url_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_is_crawled ON pages(is_crawled)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_domain ON pages(domain)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_target_hash ON links(target_url_hash)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_link_type ON links(link_type)")
        
        self.connection.commit()
        logger.info(f"Database initialized: {self.db_path}")
    
    def url_hash(self, url: str) -> str:
        """Generate hash for URL"""
        return hashlib.sha256(url.encode()).hexdigest()
    
    def add_page(self, url: str, normalized_url: str, parent_url: str = None, depth: int = 0) -> Optional[int]:
        """
        Add page to the database if its normalized version doesn't exist yet.
        Returns the ID of the existing or newly inserted page.
        """
        cursor = self.connection.cursor()
        normalized_url_hash = self.url_hash(normalized_url)

        # First, check if a page with this normalized hash already exists
        cursor.execute("SELECT id FROM pages WHERE normalized_url_hash = ?", (normalized_url_hash,))
        result = cursor.fetchone()
        if result:
            return result[0]

        # If it doesn't exist, insert the new page
        url_hash = self.url_hash(url)
        parsed = urlparse(url)
        try:
            cursor.execute("""
                INSERT INTO pages
                (url, url_hash, normalized_url, normalized_url_hash, domain, scheme, path, query_string,
                 fragment, discovered_at, crawl_depth, parent_url)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                url, url_hash, normalized_url, normalized_url_hash, parsed.netloc, parsed.scheme,
                parsed.path, parsed.query, parsed.fragment, datetime.now(),
                depth, parent_url
            ))
            self.connection.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # This could happen in a race condition if another thread inserted the same normalized_url_hash
            # between our SELECT and INSERT. We can now safely assume it exists.
            logger.warning(f"IntegrityError on insert for url {url} (normalized: {normalized_url}), re-querying.")
            cursor.execute("SELECT id FROM pages WHERE normalized_url_hash = ?", (normalized_url_hash,))
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
                 xpath, css_selector, detected_method,
                 is_javascript, is_dynamic, onclick_handler, href_attribute,
                 data_attributes, aria_label, surrounding_text, link_context,
                 discovered_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                source_page_id, link_data['target_url'], target_hash,
                link_data.get('text'), link_data.get('title'),
                link_data.get('type'), link_data.get('rel'),
                link_data.get('is_internal'), link_data.get('is_follow'),
                link_data.get('is_external'), link_data.get('xpath'),
                link_data.get('css_selector'),
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
             mime_type, discovered_at, source_tag, source_attribute, alt_text, media_keywords)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            page_id, resource_data.get('url'), resource_data.get('type'),
            resource_data.get('size'), resource_data.get('load_time'),
            resource_data.get('mime_type'), datetime.now(),
            resource_data.get('source_tag'), resource_data.get('source_attribute'),
            resource_data.get('alt_text'), resource_data.get('media_keywords')
        ))
        self.connection.commit()
    
    def get_next_uncrawled(self, domain: Optional[str] = None) -> Optional[tuple]:
        """Get next uncrawled page, optionally for a specific domain."""
        cursor = self.connection.cursor()
        query = """
            SELECT id, url, crawl_depth 
            FROM pages 
            WHERE is_crawled = 0 
        """
        params = []
        if domain:
            query += " AND domain = ?"
            params.append(domain)

        query += " ORDER BY crawl_depth ASC, discovered_at ASC LIMIT 1"

        cursor.execute(query, params)
        return cursor.fetchone()

    def get_distinct_domains(self) -> List[str]:
        """Get a list of distinct domains from the pages table"""
        cursor = self.connection.cursor()
        cursor.execute("SELECT DISTINCT domain FROM pages WHERE domain IS NOT NULL ORDER BY domain")
        return [row[0] for row in cursor.fetchall()]

    def get_total_pages_count(self, domain: Optional[str] = None) -> int:
        """Returns the total number of pages, optionally for a specific domain."""
        cursor = self.connection.cursor()
        query = "SELECT COUNT(id) FROM pages"
        params = []
        if domain:
            query += " WHERE domain = ?"
            params.append(domain)
        cursor.execute(query, params)
        return cursor.fetchone()[0]

    def get_crawled_pages_count(self, domain: Optional[str] = None) -> int:
        """Returns the number of crawled pages, optionally for a specific domain."""
        cursor = self.connection.cursor()
        query = "SELECT COUNT(id) FROM pages WHERE is_crawled = 1"
        params = []
        if domain:
            query += " AND domain = ?"
            params.append(domain)
        cursor.execute(query, params)
        return cursor.fetchone()[0]

    def get_uncrawled_pages_count(self, domain: Optional[str] = None) -> int:
        """Returns the number of uncrawled pages, optionally for a specific domain."""
        cursor = self.connection.cursor()
        query = "SELECT COUNT(id) FROM pages WHERE is_crawled = 0"
        params = []
        if domain:
            query += " AND domain = ?"
            params.append(domain)
        cursor.execute(query, params)
        return cursor.fetchone()[0]

    def delete_domain_data(self, domain: str):
        """Deletes all data associated with a specific domain."""
        cursor = self.connection.cursor()
        # First, delete from child tables (links, resources, etc.)
        cursor.execute("DELETE FROM links WHERE source_page_id IN (SELECT id FROM pages WHERE domain = ?)", (domain,))
        cursor.execute("DELETE FROM javascript_events WHERE page_id IN (SELECT id FROM pages WHERE domain = ?)", (domain,))
        cursor.execute("DELETE FROM resources WHERE page_id IN (SELECT id FROM pages WHERE domain = ?)", (domain,))
        # Finally, delete from the pages table
        cursor.execute("DELETE FROM pages WHERE domain = ?", (domain,))
        self.connection.commit()
        logger.info(f"All data for domain '{domain}' has been deleted.")

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

    def normalize_url_advanced(self, url: str, current_url: str) -> Optional[str]:
        """
        Performs advanced normalization on a URL:
        - Resolves relative URLs to absolute.
        - Converts scheme and netloc to lowercase.
        - Removes default ports (80 for http, 443 for https).
        - Removes fragment identifiers.
        - Sorts query parameters alphabetically.
        - Removes common tracking parameters.
        """
        if not url or url.startswith(('#', 'javascript:', 'mailto:', 'tel:')):
            return None

        absolute_url = urljoin(current_url, url)
        parsed = urlparse(absolute_url)

        # Common tracking parameters to remove
        tracking_params = {'utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
                           'gclid', 'fbclid', 'msclkid'}

        # Scheme and netloc to lowercase
        scheme = parsed.scheme.lower()
        netloc = parsed.netloc.lower()

        # Remove default ports
        if (scheme == 'http' and netloc.endswith(':80')) or \
           (scheme == 'https' and netloc.endswith(':443')):
            netloc = netloc.rsplit(':', 1)[0]

        # Sort and filter query parameters
        query_params = parse_qs(parsed.query)
        sorted_filtered_params = sorted(
            (k, v) for k, v in query_params.items() if k not in tracking_params
        )

        # Rebuild query string
        encoded_query = urlencode(sorted_filtered_params, doseq=True)

        path = parsed.path if parsed.path else '/'

        # Reconstruct URL without fragment
        normalized = urlunparse((scheme, netloc, path, '', encoded_query, ''))

        return normalized
    
    def extract_static_links(self, soup: BeautifulSoup, current_url: str) -> List[Dict]:
        """Extract static HTML links"""
        links = []
        
        # <a> tags
        for tag in soup.find_all('a', href=True):
            url = self.normalize_url(tag['href'], current_url)
            if url:
                rel_val = tag.get('rel')
                rel_str = ' '.join(rel_val) if isinstance(rel_val, list) else rel_val
                is_follow = 'nofollow' not in (rel_val or [])

                links.append({
                    'target_url': url,
                    'text': tag.get_text(strip=True)[:500],
                    'title': tag.get('title'),
                    'type': 'anchor',
                    'rel': rel_str,
                    'is_internal': self.is_internal(url),
                    'is_follow': is_follow,
                    'is_external': not self.is_internal(url),
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('href'),
                    'aria_label': tag.get('aria-label'),
                    'data_attributes': json.dumps({k: v for k, v in tag.attrs.items() if k.startswith('data-')})
                })
        
        # <link> tags
        for tag in soup.find_all('link', href=True):
            url = self.normalize_url(tag['href'], current_url)
            if url:
                rel_val = tag.get('rel')
                rel_str = ' '.join(rel_val) if isinstance(rel_val, list) else rel_val

                links.append({
                    'target_url': url,
                    'type': 'link_tag',
                    'rel': rel_str,
                    'is_internal': self.is_internal(url),
                    'is_follow': True,
                    'is_external': not self.is_internal(url),
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('href')
                })
        
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
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('action')
                })
        
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
                    'detected_method': 'static_html',
                    'is_javascript': False,
                    'is_dynamic': False,
                    'href': tag.get('src')
                })
        
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
                    'detected_method': 'onclick_attribute',
                    'is_javascript': True,
                    'is_dynamic': False,
                    'onclick': onclick[:1000]
                })
        
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
                    'detected_method': 'javascript_code',
                    'is_javascript': True,
                    'is_dynamic': False,
                    'context': script_content[:500] if len(script_content) > 500 else script_content
                })
        
        return links


class ResourceExtractor:
    """Extracts various resources from a BeautifulSoup object."""

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc

    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalize and resolve URL"""
        if not url or url.startswith(('javascript:', 'mailto:', 'tel:')):
            return None
        return urljoin(self.base_url, url)

    def extract_all_resources(self, soup: BeautifulSoup) -> List[Dict]:
        """Extract all supported resources from the page."""
        resources = []
        resources.extend(self.extract_images(soup))
        resources.extend(self.extract_videos(soup))
        resources.extend(self.extract_audios(soup))
        resources.extend(self.extract_documents(soup))
        resources.extend(self.extract_scripts(soup))
        resources.extend(self.extract_stylesheets(soup))
        resources.extend(self.extract_favicons(soup))
        resources.extend(self.extract_embedded_content(soup))
        return resources

    def extract_images(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts image resources."""
        images = []
        # <img> tags
        for tag in soup.find_all('img'):
            src = self._normalize_url(tag.get('src'))
            if src:
                images.append({
                    'url': src,
                    'type': 'image',
                    'source_tag': 'img',
                    'source_attribute': 'src',
                    'alt_text': tag.get('alt'),
                    'media_keywords': 'image, img'
                })
        # <picture> tags
        for tag in soup.find_all('picture'):
            for source in tag.find_all('source'):
                srcset = self._normalize_url(source.get('srcset'))
                if srcset:
                    images.append({
                        'url': srcset,
                        'type': 'image',
                        'source_tag': 'picture',
                        'source_attribute': 'srcset',
                        'media_keywords': 'image, picture, source'
                    })
        # Background images from inline styles
        for tag in soup.find_all(style=True):
            style = tag['style']
            match = re.search(r'url\((.*?)\)', style)
            if match:
                url = self._normalize_url(match.group(1).strip('\'"'))
                if url:
                    images.append({
                        'url': url,
                        'type': 'image',
                        'source_tag': tag.name,
                        'source_attribute': 'style',
                        'media_keywords': 'image, background-image, css'
                    })
        return images

    def extract_videos(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts video resources."""
        videos = []
        for tag in soup.find_all('video'):
            src = self._normalize_url(tag.get('src'))
            if src:
                videos.append({
                    'url': src,
                    'type': 'video',
                    'source_tag': 'video',
                    'source_attribute': 'src',
                    'media_keywords': 'video'
                })
            for source in tag.find_all('source'):
                src = self._normalize_url(source.get('src'))
                if src:
                    videos.append({
                        'url': src,
                        'type': 'video',
                        'source_tag': 'source',
                        'source_attribute': 'src',
                        'media_keywords': 'video, source'
                    })
        return videos

    def extract_audios(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts audio resources."""
        audios = []
        for tag in soup.find_all('audio'):
            src = self._normalize_url(tag.get('src'))
            if src:
                audios.append({
                    'url': src,
                    'type': 'audio',
                    'source_tag': 'audio',
                    'source_attribute': 'src',
                    'media_keywords': 'audio'
                })
            for source in tag.find_all('source'):
                src = self._normalize_url(source.get('src'))
                if src:
                    audios.append({
                        'url': src,
                        'type': 'audio',
                        'source_tag': 'source',
                        'source_attribute': 'src',
                        'media_keywords': 'audio, source'
                    })
        return audios

    def extract_documents(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts links to documents."""
        docs = []
        doc_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip', '.rar']
        for tag in soup.find_all('a', href=True):
            href = tag['href']
            if any(href.lower().endswith(ext) for ext in doc_extensions):
                url = self._normalize_url(href)
                if url:
                    docs.append({
                        'url': url,
                        'type': 'document',
                        'source_tag': 'a',
                        'source_attribute': 'href',
                        'media_keywords': f"document, {href.split('.')[-1]}"
                    })
        return docs

    def extract_scripts(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts script resources."""
        scripts = []
        for tag in soup.find_all('script', src=True):
            src = self._normalize_url(tag.get('src'))
            if src:
                scripts.append({
                    'url': src,
                    'type': 'script',
                    'source_tag': 'script',
                    'source_attribute': 'src',
                    'media_keywords': 'script, javascript, js'
                })
        return scripts

    def extract_stylesheets(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts stylesheet resources."""
        styles = []
        for tag in soup.find_all('link', rel='stylesheet', href=True):
            href = self._normalize_url(tag.get('href'))
            if href:
                styles.append({
                    'url': href,
                    'type': 'stylesheet',
                    'source_tag': 'link',
                    'source_attribute': 'href',
                    'media_keywords': 'stylesheet, css'
                })
        return styles

    def extract_favicons(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts favicon resources."""
        favicons = []
        for tag in soup.find_all('link', rel=lambda r: r and 'icon' in r, href=True):
            href = self._normalize_url(tag.get('href'))
            if href:
                favicons.append({
                    'url': href,
                    'type': 'favicon',
                    'source_tag': 'link',
                    'source_attribute': 'href',
                    'media_keywords': 'favicon, icon'
                })
        return favicons

    def extract_embedded_content(self, soup: BeautifulSoup) -> List[Dict]:
        """Extracts embedded content like iframes, embeds, and objects."""
        embedded = []
        for tag_name in ['iframe', 'embed', 'object']:
            for tag in soup.find_all(tag_name):
                src_attr = 'src' if tag_name != 'object' else 'data'
                src = self._normalize_url(tag.get(src_attr))
                if src:
                    embedded.append({
                        'url': src,
                        'type': f'embedded_{tag_name}',
                        'source_tag': tag_name,
                        'source_attribute': src_attr,
                        'media_keywords': f'embedded, {tag_name}'
                    })
        return embedded


class UrlTrapDetector:
    """Detects URL patterns that are likely to be crawler traps."""
    def __init__(self, max_path_depth=10, max_repeating_segments=3, max_query_variations=5):
        self.max_path_depth = max_path_depth
        self.max_repeating_segments = max_repeating_segments
        self.max_query_variations = max_query_variations

        # Stores {path_without_query: {query_param_key_tuple, ...}}
        self.path_query_structures: Dict[str, Set[tuple]] = {}

    def is_trap(self, url: str) -> bool:
        """Check if a URL is a potential trap."""
        parsed = urlparse(url)
        path = parsed.path

        # 1. Check for excessive path depth
        path_segments = [seg for seg in path.split('/') if seg]
        if len(path_segments) > self.max_path_depth:
            logger.warning(f"Trap detected: Excessive path depth in {url}")
            return True

        # 2. Check for repeating path segments
        path_segment_counts = {}
        for segment in path_segments:
            path_segment_counts[segment] = path_segment_counts.get(segment, 0) + 1
            if path_segment_counts[segment] > self.max_repeating_segments:
                logger.warning(f"Trap detected: Repeating path segment '{segment}' in {url}")
                return True

        # 3. Check for too many query variations for the same path
        path_base = parsed.path
        query_params = parse_qs(parsed.query)
        # Create a frozenset of parameter keys to represent the query structure
        query_structure = frozenset(query_params.keys())

        known_structures = self.path_query_structures.get(path_base, set())

        if query_structure in known_structures:
            return False  # Not a new trap, we've seen this structure before

        # It's a new structure, check if adding it would exceed the limit
        if len(known_structures) >= self.max_query_variations:
            logger.warning(f"Trap detected: Excessive query variations for path '{path_base}' on new structure")
            return True  # This new structure would be the N+1th variation

        # If not a trap, add it to the known structures for future checks
        if path_base not in self.path_query_structures:
            self.path_query_structures[path_base] = {query_structure}
        else:
            self.path_query_structures[path_base].add(query_structure)

        return False


class WebCrawler:
    """Main crawler class"""

    def __init__(self, db_manager: DatabaseManager, start_url: Optional[str] = None,
                 domain_to_crawl: Optional[str] = None, max_depth: int = 3,
                 delay: float = 1.0, use_selenium: bool = True, disregard_robots: bool = False):
        self.db = db_manager
        self.start_url = start_url
        self.domain_to_crawl = domain_to_crawl
        self.max_depth = max_depth
        self.delay = delay
        self.use_selenium = use_selenium
        self.disregard_robots = disregard_robots
        
        if start_url:
            base_for_detector = start_url
        elif domain_to_crawl:
            base_for_detector = f"http://{domain_to_crawl}"
        else:
            raise ValueError("WebCrawler requires either a start_url or a domain_to_crawl.")

        self.link_detector = LinkDetector(base_for_detector)
        self.resource_extractor = ResourceExtractor(base_for_detector)
        self.trap_detector = UrlTrapDetector()
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (AdvancedCrawler/1.0)'
        self.robot_parsers = {}
        
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': self.user_agent})
        
        self.driver = None
        if use_selenium:
            self.setup_selenium()

    def print_initial_summary(self):
        """Prints a summary of the current database state."""
        total_pages = self.db.get_total_pages_count()
        crawled_pages = self.db.get_crawled_pages_count()
        uncrawled_pages = self.db.get_uncrawled_pages_count()
        domains = len(self.db.get_distinct_domains())

        print("\n--- Database Initial State ---")
        print(f"Total domains: {domains}")
        print(f"Total pages discovered: {total_pages}")
        print(f"Pages crawled: {crawled_pages}")
        print(f"Pages pending crawl: {uncrawled_pages}")
        print("------------------------------\n")

    def parse_sitemap(self, domain: str):
        """Finds, fetches, and parses the sitemap(s) for a domain."""
        robot_parser = self.get_robot_parser(domain)
        sitemap_urls = []
        if robot_parser and robot_parser.sitemaps:
            sitemap_urls.extend(robot_parser.sitemaps)
        else:
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
            namespace = {'ns': root.tag.split('}')[0][1:]} if '}' in root.tag else {'ns': ''}
            urls = [loc.text.strip() for loc in root.findall('.//ns:loc', namespaces=namespace)]
            for url in urls:
                if self.link_detector.is_internal(url):
                    normalized_url = self.link_detector.normalize_url_advanced(url, self.link_detector.base_url)
                    if normalized_url:
                        self.db.add_page(url, normalized_url, parent_url='sitemap', depth=0)
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
            self.robot_parsers[domain] = None
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
            
            driver_name = "chromedriver.exe" if platform.system() == "Windows" else "chromedriver"
            local_driver_path = os.path.abspath(driver_name)

            if os.path.exists(local_driver_path):
                logger.info(f"Using local chromedriver from: {local_driver_path}")
                service = Service(executable_path=local_driver_path)
            else:
                logger.info("Local chromedriver not found, downloading...")
                service = Service(ChromeDriverManager().install())

            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            logger.info("Selenium WebDriver initialized")
        except Exception as e:
            logger.warning(f"Could not initialize Selenium: {e}")
            self.use_selenium = False
    
    def _process_page_content(self, soup: BeautifulSoup, url: str, page_id: int, depth: int, all_links: List[Dict]) -> (Dict, List):
        """
        Extracts metadata from soup, processes links and resources.
        Returns extracted metadata and a list of new internal pages to be queued.
        """
        page_metadata = {}
        new_internal_pages = []

        # Extract metadata from the soup object
        page_metadata['title'] = soup.title.string if soup.title else None
        meta_desc = soup.find('meta', attrs={'name': 'description'})
        page_metadata['meta_description'] = meta_desc['content'] if meta_desc else None
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        page_metadata['meta_keywords'] = meta_keywords['content'] if meta_keywords else None
        canonical = soup.find('link', attrs={'rel': 'canonical'})
        page_metadata['canonical_url'] = canonical['href'] if canonical else None
        robots = soup.find('meta', attrs={'name': 'robots'})
        page_metadata['robots_meta'] = robots['content'] if robots else None
        og_title = soup.find('meta', property='og:title')
        page_metadata['og_title'] = og_title['content'] if og_title else None
        og_desc = soup.find('meta', property='og:description')
        page_metadata['og_description'] = og_desc['content'] if og_desc else None
        og_img = soup.find('meta', property='og:image')
        page_metadata['og_image'] = og_img['content'] if og_img else None
        og_type = soup.find('meta', property='og:type')
        page_metadata['og_type'] = og_type['content'] if og_type else None
        tw_card = soup.find('meta', attrs={'name': 'twitter:card'})
        page_metadata['twitter_card'] = tw_card['content'] if tw_card else None
        lang = soup.find('html')
        page_metadata['language'] = lang.get('lang') if lang else None

        # Process links
        for link in all_links:
            self.db.add_link(page_id, link)
            normalized_url = self.link_detector.normalize_url_advanced(link['target_url'], url)
            if not normalized_url or self.trap_detector.is_trap(normalized_url):
                continue

            if link['is_internal']:
                if depth < self.max_depth:
                    # Return to the worker to be added to the DB and queue
                    new_internal_pages.append((link['target_url'], normalized_url, url, depth + 1))
            else:
                # External links are added directly, as they don't go into the current crawl queue
                self.db.add_page(link['target_url'], normalized_url, parent_url=None, depth=0)

        # Process resources
        all_resources = self.resource_extractor.extract_all_resources(soup)
        for resource in all_resources:
            self.db.add_resource(page_id, resource)

        logger.debug(f"Processed {len(all_links)} links and {len(all_resources)} resources on {url}")
        return page_metadata, new_internal_pages

    def crawl_page_static(self, url: str, page_id: int, depth: int) -> (Dict, List):
        """Crawl page using requests"""
        start_time = time.time()
        page_data = {}
        new_pages = []
        
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
                all_links = self.link_detector.extract_static_links(soup, url) + \
                            self.link_detector.extract_javascript_links(soup, url)
                
                content_data, new_pages = self._process_page_content(soup, url, page_id, depth, all_links)
                page_data.update(content_data)
        
        except Exception as e:
            logger.error(f"Error crawling {url}: {e}")
            page_data['error_message'] = str(e)
        
        return page_data, new_pages
    
    def crawl_page_selenium(self, url: str, page_id: int, depth: int) -> (Dict, List):
        """Crawl page using Selenium for dynamic content"""
        page_data = {}
        new_pages = []
        start_time = time.time()
        try:
            self.driver.get(url)
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(2)
            
            page_data['response_time_ms'] = int((time.time() - start_time) * 1000)
            page_source = self.driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            static_links = self.link_detector.extract_static_links(soup, url)
            js_links = self.link_detector.extract_javascript_links(soup, url)
            dynamic_links = []
            
            clickable_elements = self.driver.find_elements(By.XPATH, "//*[@onclick or @href or contains(@class, 'link') or contains(@class, 'btn')]")
            for element in clickable_elements[:100]:
                try:
                    href = element.get_attribute('href')
                    onclick = element.get_attribute('onclick')
                    detected_url = None
                    if href:
                        detected_url = self.link_detector.normalize_url(href, url)
                    elif onclick:
                        urls = self.link_detector.extract_urls_from_js(onclick, url)
                        detected_url = urls[0] if urls else None
                    if detected_url:
                        dynamic_links.append({
                            'target_url': detected_url, 'text': element.text[:500], 'type': 'dynamic',
                            'is_internal': self.link_detector.is_internal(detected_url), 'is_follow': True,
                            'is_external': not self.link_detector.is_internal(detected_url),
                            'detected_method': 'selenium', 'is_javascript': bool(onclick),
                            'is_dynamic': True, 'onclick': onclick
                        })
                except Exception:
                    continue
            
            all_links = static_links + js_links + dynamic_links
            content_data, new_pages = self._process_page_content(soup, url, page_id, depth, all_links)
            page_data.update(content_data)

            page_data['title'] = self.driver.title
            page_data['status_code'] = 200

        except Exception as e:
            logger.error(f"Error with Selenium on {url}: {e}")
            page_data['error_message'] = str(e)
        return page_data, new_pages
    
    def crawl_page(self, url: str, page_id: int, depth: int) -> (Dict, List):
        """Crawl a single page and return its data and any new internal links found."""
        logger.debug(f"Crawling: {url} (depth: {depth})")
        
        page_data, new_pages = self.crawl_page_selenium(url, page_id, depth) if self.use_selenium and self.driver \
                    else self.crawl_page_static(url, page_id, depth)
        
        time.sleep(self.delay)
        return page_data, new_pages

    
    def cleanup(self):
        """Cleanup resources"""
        if self.driver:
            self.driver.quit()


class CrawlerManager:
    """Orchestrates the asynchronous crawling process."""
    def __init__(self, db_manager: DatabaseManager, args: argparse.Namespace):
        self.db = db_manager
        self.args = args
        self.queue = asyncio.Queue()
        self.executor = ThreadPoolExecutor(max_workers=args.workers)
        self.crawler = None
        self.domain_to_crawl = None
        self.in_queue = set()
        self.in_queue_lock = asyncio.Lock()
        self.crawled_count = 0
        self.start_time = None

    async def worker(self, name: str):
        """The worker task that processes URLs from the queue."""
        while True:
            try:
                page_id, url, depth = await self.queue.get()

                # Check robots.txt and max depth
                if not self.args.disregard_robots:
                    domain = urlparse(url).netloc
                    robot_parser = self.crawler.get_robot_parser(domain)
                    if robot_parser and not robot_parser.can_fetch(self.crawler.user_agent, url):
                        logger.warning(f"Worker {name} skipping (disallowed by robots.txt): {url}")
                        self.db.update_page_crawl(page_id, {'status_code': 403, 'error_message': 'Disallowed by robots.txt'})
                        self.queue.task_done()
                        continue

                if depth > self.args.max_depth:
                    logger.info(f"Worker {name} skipping (max depth reached): {url}")
                    self.db.update_page_crawl(page_id, {'status_code': 0, 'error_message': 'Max depth reached'})
                    self.queue.task_done()
                    continue

                logger.info(f"Worker {name} processing: {url} (depth: {depth})")

                loop = asyncio.get_running_loop()
                page_data, new_pages = await loop.run_in_executor(
                    self.executor, self.crawler.crawl_page, url, page_id, depth
                )

                self.db.update_page_crawl(page_id, page_data)
                self.crawled_count += 1

                # Add newly discovered internal pages to the queue
                for new_url, new_norm_url, parent_url, new_depth in new_pages:
                    new_page_id = self.db.add_page(new_url, new_norm_url, parent_url=parent_url, depth=new_depth)
                    if new_page_id:
                        async with self.in_queue_lock:
                            if new_page_id not in self.in_queue:
                                self.in_queue.add(new_page_id)
                                await self.queue.put((new_page_id, new_url, new_depth))

                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Worker {name} encountered an error on {url}: {e}")
                if not self.queue.empty():
                    self.queue.task_done()

    async def run(self):
        """Sets up and starts the crawling process."""
        self.start_time = time.time()
        crawler = await self.setup_crawler()
        if not crawler:
            return
        
        self.crawler = crawler
        self.crawler.print_initial_summary()

        # Populate the queue with initial URLs
        page = self.db.get_next_uncrawled(self.domain_to_crawl)
        while page:
            page_id, url, depth = page
            async with self.in_queue_lock:
                if page_id not in self.in_queue:
                    self.in_queue.add(page_id)
                    await self.queue.put(page)
            page = self.db.get_next_uncrawled(self.domain_to_crawl)

        initial_pages = self.queue.qsize()
        if initial_pages == 0:
            logger.info("No pages to crawl for the selected task.")
            if self.crawler: self.crawler.cleanup()
            return

        logger.info(f"Populated queue with {initial_pages} pages for target: {self.domain_to_crawl or 'all domains'}")

        tasks = [asyncio.create_task(self.worker(f'worker-{i+1}')) for i in range(self.args.workers)]

        await self.queue.join()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        duration = time.time() - self.start_time
        logger.info(f"Crawl finished. Crawled {self.crawled_count} pages in {duration:.2f} seconds.")

        if self.crawler:
            self.crawler.cleanup()

    async def setup_crawler(self) -> Optional[WebCrawler]:
        """Handles the command-line arguments to configure the crawler."""
        if self.args.new_scan:
            start_url = self.args.new_scan
            domain = urlparse(start_url).netloc
            if self.db.get_total_pages_count(domain) > 0:
                choice = input(f"Data for domain '{domain}' already exists. Delete it and start a fresh scan? (y/n): ").lower()
                if choice != 'y':
                    print("Aborting scan.")
                    return None
                self.db.delete_domain_data(domain)

            self.domain_to_crawl = domain
            crawler = WebCrawler(
                db_manager=self.db, start_url=start_url, domain_to_crawl=domain,
                max_depth=self.args.max_depth, delay=self.args.delay,
                use_selenium=self.args.use_selenium, disregard_robots=self.args.disregard_robots
            )
            normalized_start_url = crawler.link_detector.normalize_url_advanced(start_url, start_url)
            self.db.add_page(start_url, normalized_start_url, depth=0)
            crawler.parse_sitemap(domain)
            return crawler

        elif self.args.update:
            domains = self.db.get_distinct_domains()
            if not domains:
                print("No domains found in the database to update.")
                return None

            print("Please choose a domain to update:")
            for i, domain in enumerate(domains):
                print(f"{i + 1}: {domain}")

            try:
                choice = int(input("Enter the number of the domain: ")) - 1
                if 0 <= choice < len(domains):
                    self.domain_to_crawl = domains[choice]
                    print(f"Resetting and preparing to update domain: {self.domain_to_crawl}")
                    self.db.reset_domain_crawl_status(self.domain_to_crawl)
                else:
                    print("Invalid choice.")
                    return None
            except (ValueError, IndexError):
                print("Invalid input.")
                return None

        elif self.args.continue_crawl:
            if self.db.get_uncrawled_pages_count() == 0:
                print("No pages left to crawl in the database.")
                return None
            self.domain_to_crawl = None  # Crawl all domains

        # Default crawler for update/continue
        return WebCrawler(
            db_manager=self.db, domain_to_crawl=self.domain_to_crawl,
            max_depth=self.args.max_depth, delay=self.args.delay,
            use_selenium=self.args.use_selenium, disregard_robots=self.args.disregard_robots
        )


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Advanced Web Crawler with different modes of operation.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--new-scan", metavar="URL", help="Start a new scan from a URL. Deletes old data for the domain if it exists.")
    mode_group.add_argument("--update", action="store_true", help="Update an existing domain by re-crawling all its pages.")
    mode_group.add_argument("--continue-crawl", action="store_true", help="Continue the last crawl, processing any remaining uncrawled links.")

    parser.add_argument("-d", "--max-depth", type=int, default=3, help="Maximum crawl depth. Default: 3")
    parser.add_argument("-w", "--delay", type=float, default=1.0, help="Delay between requests in seconds. Default: 1.0")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent crawler workers. Default: 4")
    parser.add_argument("-s", "--use-selenium", action='store_true', help="Use Selenium for dynamic content (Note: Selenium runs sequentially, not in parallel).")
    parser.add_argument("--disregard-robots", action='store_true', help="Disregard robots.txt rules.")
    
    args = parser.parse_args()
    db_manager = DatabaseManager()

    # The new async manager will handle the logic, replacing the old synchronous flow
    manager = CrawlerManager(db_manager, args)

    try:
        asyncio.run(manager.run())
    except KeyboardInterrupt:
        logger.info("Crawler stopped by user.")
    except Exception as e:
        logger.error(f"An unexpected error occurred in manager: {e}")
    finally:
        db_manager.close()
        print("\nOperation finished.")


if __name__ == "__main__":
    main()
