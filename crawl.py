import io
import sys
import gzip
import json
import time
import signal
import hashlib
import logging
import argparse

from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urlunparse

import lxml

from bs4 import BeautifulSoup as bs


class HTTPReq:

    def __init__(self, timeout=10, base_headers={}):

        self.base_headers = base_headers
        self.timeout = timeout


    def send_req(self, req):

        method = req.get_method()

        logging.info('%s %s', method, req.get_full_url())

        try:
            resp = urlopen(req, timeout=self.timeout)
        except URLError as exc:
            logging.error('Protocol error: %s', exc)
            return None
        except HTTPError as exc:
            logging.error('Request %s', exc)
            return exc
        except TimeoutError as exc:
            logging.error('Request timed out: %s', exc)
            return None
        except Exception as exc:
            logging.exception('%s failed: %s', method, exc)
            return None

        return resp


    def head_req(self, url, headers={}):

        headers.update(self.base_headers)
        req = Request(url, headers=headers, method='HEAD')

        return self.send_req(req)


    def get_req(self, url, headers={}):

        headers.update(self.base_headers)
        req = Request(url, headers=headers, method='GET')

        return self.send_req(req)


class CrawlSite(HTTPReq):

    def __init__(self, host, user_agent='CrawlSite', obey_robots=True, files_path=None, gzip_files=False, markup=True, images=False, timeout=10, cache_limit=86400):
        logging.debug('CrawlSite.__init__()')

        self.ua = user_agent
        self.base_headers = {'User-Agent': self.ua}
        self.timeout = timeout
        self.cache_limit = cache_limit
        self.next_req = time.time()
        self.crawl_delay = 2

        super().__init__(timeout=self.timeout, base_headers=self.base_headers)

        self.host = host
        self.url = f'https://{host}'

        self.pages = {}
        self.sitemap = []

        self.markup = markup

        self.obey_robots = obey_robots
        self.rp = RobotFileParser()

        if files_path is None:
            files_path = self.host.replace('.', '_') + '_files'

        self.files = Path(files_path)
        self.files.mkdir(exist_ok=True)
        self.gzip = gzip_files

        signal.signal(signal.SIGINT, self.clean_shutdown)


    def wait(self):
        """Processes take time, so figure out the time when the next request should be
           while respecting the rate limit / crawl delay, but count process time as part
           of the wait. In other words, process while waiting,  don't process and then
           wait the crawl delay.
        """
        logging.debug('CrawlSite.wait()')
        wait = self.next_req - time.time()
        if wait > 0:
            time.sleep(wait)
        self.next_req = time.time() + self.crawl_delay


    def clean_shutdown(self, sig, frame):
        logging.debug('CrawlSite.clean_shutdown()')

        logging.info('Executing clean shutdown')
        self.save_session()

        logging.info('Clean shutdown complete')
        sys.exit(0)


    def load_robots(self, robots_file=None):
        logging.debug('CrawlSite.load_robots()')

        if robots_file is None:
            robots_url = f'https://{self.host}/robots.txt'
        else:
            robots_url = f'{self.url}/{robots_file}'

        logging.info("Loading site's robots.txt file: %s", robots_url)

        self.rp.set_url(robots_url)

        resp = self.get_req(robots_url)
        if resp is None:
            logging.info('No robots.txt found for %s', self.host)
            self.obey_robots = False
            return

        robots = resp.read()

        lines = robots.decode("utf-8").splitlines()

        if lines:
            self.rp.parse(lines)

        sleep_for = self.rp.crawl_delay(self.ua)
        self.crawl_delay = self.crawl_delay if sleep_for is None else sleep_for

        sm = self.rp.site_maps()
        if sm is not None:
            self.sitemap = sm

        for sitemap in self.sitemap:
            if sitemap not in self.sitemap:
                self.sitemap.append(sitemap)

        self.load_sitemap()


    def load_sitemap(self, map_url=[]):
        logging.debug('CrawlSite.load_sitemap()')

        headers = {'User-Agent': self.ua}

        if not map_url:
            map_url = self.sitemap

        for sitemapurl in map_url:

            sitemap_raw = self.get_req(sitemapurl, headers=headers)
            soup = bs(sitemap_raw, 'lxml')

            for link in soup.find_all('loc'):
                self.add_link(link.text)


    def add_link(self, href):
        logging.debug('CrawlSite.add_link()')

        if href not in self.pages or ((time.time() - self.cache_limit) > self.pages[href].setdefault('last_visit', self.cache_limit)):
            if href not in self.pages:
                new_page = True
                expired = False
            else:
                new_page = False
                expired = (time.time() - self.cache_limit) > self.pages[href].setdefault('last_visit', self.cache_limit)
            logging.debug('Adding link: New page (%s) or Expired (%s)', new_page, expired)

            self.pages[href] = {
                'last_visit': time.time() + self.cache_limit,
                'links': {},
                'malformed': [],
                'status_code': None,
                'images': [],
                'files': [],
                'mime_type': '',
                'head_status': None,
                }

            self.wait()
            head = self.head_req(href)

            if head is not None:
                mime = head.headers.get('content-type')
                status = int(head.status)
                self.pages[href]['mime_type'] = mime
                self.pages[href]['head_status'] = status


    def crawl(self):
        logging.debug('CrawlSite.crawl()')
        logging.info('Crawling site %s with %s second delay between requests', self.host, self.crawl_delay)

        if not self.pages:
            base_url = f'https://{self.host}/'
            if not self.obey_robots or self.rp.can_fetch(self.ua, base_url):
                self.add_link(base_url)

        idx = 0
        crawl_pages = list(self.pages.keys())
        while idx < len(crawl_pages):
            crawl_url = crawl_pages[idx]
            idx += 1

            if not crawl_url.startswith(f'https://{self.host}'):
                logging.info('Not crawling external link: %s', crawl_url)
                continue

            if time.time() < self.pages[crawl_url]['last_visit'] and self.pages[crawl_url]['status_code'] is not None:
                logging.debug('Skipping cached page: %s', crawl_url)
                continue

            if self.obey_robots and not self.rp.can_fetch(self.ua, crawl_url):
                logging.info('URL fetch prevented by robots.txt: %s', crawl_url)
                continue

            self.extract_links(crawl_url)
            crawl_pages = list(self.pages.keys())

        logging.info('Crawl complete')


    def extract_links(self, target_url):
        logging.debug('CrawlSite.extract_links()')
        logging.info('Extracting links from: %s', target_url)

        if 'text/html' not in self.pages[target_url]['mime_type']:
            logging.info('Skipping invalid MIME type: %s', self.pages[target_url]['mime_type'])
            return

        if self.pages[target_url]['status_code'] is not None and self.pages[target_url]['last_visit'] > time.time():
            logging.debug('Skipping visited page')
            return

        self.wait()
        resp = self.get_req(target_url)
        if resp is None:
            return

        self.pages[target_url]['last_visit'] = time.time() + self.cache_limit
        self.pages[target_url]['status_code'] = int(resp.status)

        if resp.status != 200:
            logging.info('Not parsing %s status code', resp.status)
            return

        # Add support for text/xml
        raw_html = resp.read()
        logging.debug('HTML Size: %s', len(raw_html))

        # Write HTML to file
        if self.markup:
            filename = hashlib.file_digest(io.BytesIO(raw_html), 'sha256').hexdigest() + '.html'

            if self.gzip:
                filename += '.gz'
            fullpath = Path(self.files, filename)

            self.pages[target_url]['file_loc'] = str(fullpath),

            if not fullpath.is_file():
                if self.gzip:
                    with gzip.open(fullpath, 'wb') as f:
                        f.write(raw_html)
                else:
                    with open(fullpath, 'w') as f:
                        f.write(raw_html.decode('utf-8'))

                logging.info('Wrote HTML to %s', fullpath)
            else:
                logging.info('Not overwriting existing file: %s', fullpath)

        soup = bs(raw_html, 'html.parser')

        for link in soup.find_all('a'):

            href = link.get('href')
            if not href:
                continue

            # Fix salvageable links
            if href.startswith('//'):
                # //foo.com -> https://foo.com
                href = f'https:{href}'
            elif href.startswith('/'):
                # /path-link -> https://foo.com/path-link
                href = f'https://{self.host}{href}'
            elif href.startswith('http:'):
                # http://foo.com -> https://foo.com
                href = 'https:' + href[5:]
            elif href.startswith('#'):
                # #fragment-url -> https://foo.com/#fragment-url
                href = f'https://{self.host}/{href}'
            elif href.startswith('..'):
                # ../path/123 -> https://foo.com/path/123
                href = f'https://{self.host}' + href.lstrip('..')

            try:
                parts = urlparse(href)
            except:
                logging.info('Failed parsing HREF: %s', href)
                continue

            if not all([parts.scheme, parts.netloc]):
                logging.info('Malformed HREF: "%s"', href)
                self.pages[target_url]['malformed'].append(href)
                continue

            if parts.fragment:
                href = href.replace('#'+parts.fragment, '')

            href_path = parts.path
            if href_path.startswith('../'):
                href_path = '/' + href_path.lstrip('../')

            hn = parts.hostname

            # Standardize href - No query strings or fragments
            # We only want real pages, not search results to real pages
            clean_href = 'https://'
            clean_href += hn if hn else self.host
            clean_href += href_path if href_path else '/'

            self.pages[target_url]['links'].setdefault(href, clean_href)
            self.add_link(clean_href)

        for img in soup.find_all('img'):

            src = img.get('src')
            self.pages[target_url]['images'].append(src)


    def save_session(self, filename=None):
        logging.debug('CrawlSite.save_session()')

        if filename is None:
            filename = self.host.replace('.', '_') + '.json'

        data = {
                'host': self.host,
                'pages': self.pages,
                'sitemap': self.sitemap,
                }

        json_data = json.dumps(data, indent=4)

        with open(filename, 'w') as f:
            f.write(json_data)

        logging.info('Session data saved to %s', filename)


    def load_session(self, filename=None):
        logging.debug('CrawlSite.load_session()')

        if filename is None:
            filename = self.host.replace('.', '_') + '.json'

        logging.info('Loading session from %s', filename)

        try:
            with open(filename, 'r') as f:
                raw_json = f.read()
        except Exception as exc:
            logging.info('Existing session not found at %s', filename)
            return

        try:
            data = json.loads(raw_json)
        except Exception as exc:
            logging.error('Failed loading session data: %s', exc)
            return

        lookup = {
                'host': 'host',
                'pages': 'pages',
                'sitemap': 'sitemap',
                }

        for key, attr in lookup.items():

            try:
                setattr(self, attr, data[key])
            except Exception as exc:
                logging.debug('Skipping key %s: %s', exc)
                continue

        logging.info(
                'Session loaded: Host %s :: %s links',
                self.host,
                len(self.pages)
                )


def parse_args():

    parser = argparse.ArgumentParser(description='Web Crawler')
    parser.add_argument('-s', '--site', dest='site', action='store', help='Crawls given site')
    parser.add_argument('-p', '--page', dest='page', action='store', help='Crawls given page')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', default=False, help='Show debug output')
    parser.add_argument('-z', '--gzip', dest='gzip', action='store_true', default=False, help='Store compressed HTML files')
    parser.add_argument('-n', '--no-cache', dest='nocache', action='store_true', default=False, help='Refresh cached pages')
    parser.add_argument('-c', '--cache-limt', dest='cachelimit', action='store', default=86400, type=int, help='Cache expiration limit')
    parser.add_argument('-l', '--log', dest='logfile', action='store', help='File location for log file')

    parsed = parser.parse_args()

    return parsed


if __name__ == '__main__':

    conf = parse_args()

    loglevel = logging.DEBUG if conf.debug else logging.INFO
    logging.basicConfig(level=loglevel, format='%(asctime)s [%(levelname)s] %(message)s', filename=conf.logfile)

    logging.debug('ARGS: %s', conf)

    if conf.nocache:
        cache = 0
    else:
        cache = conf.cachelimit

    if conf.page:
        url_parts = urlparse(conf.page)
        host = url_parts.hostname
        if not host:
            host = url.netloc

        crawler = CrawlSite(host=host, gzip_files=conf.gzip, cache_limit=cache)
        crawler.load_session()
        crawler.load_robots()
        crawler.extract_links(conf.page)
        crawler.save_session()

    if conf.site:
        crawler = CrawlSite(host=conf.site, gzip_files=conf.gzip, cache_limit=cache)
        crawler.load_session()
        crawler.load_robots()
        crawler.crawl()
        crawler.save_session()

