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
from urllib.error import URLError
from urllib.robotparser import RobotFileParser
from urllib.parse import urlparse, urlunparse

import lxml

from bs4 import BeautifulSoup as bs


class HTTPReq:

    def __init__(self):

        base_headers = {}


    def get_req(self, url, headers={}):

        headers.update(self.base_headers)

        req = Request(url, headers=headers)

        try:
            resp = urlopen(req)
        except:
            logging.error('Failed GET request to %s', url)
            return None

        return resp


class CrawlSite(HTTPReq):

    def __init__(self, host, user_agent='CrawlSite', obey_robots=True, files_path=None, gzip_files=False):

        super().__init__()

        self.host = host
        self.url = f'https://{host}'
        self.ua = user_agent

        self.base_headers = {'User-Agent': self.ua}

        self.visited = {}
        self.in_links = []
        self.ex_links = []
        self.ex_hosts = []
        self.sitemap = []

        self.obey_robots = obey_robots
        self.rp = RobotFileParser()

        if files_path is None:
            files_path = self.host.replace('.', '_') + '_files'

        self.files = Path(files_path)
        self.files.mkdir(exist_ok=True)
        self.gzip = gzip_files

        signal.signal(signal.SIGINT, self.clean_shutdown)


    def clean_shutdown(self, sig, frame):

        logging.info('Executing clean shutdown')
        self.save_session()

        logging.info('Clean shutdown complete')
        sys.exit(0)


    def load_robots(self, robots_file=None):

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

        for sitemap in self.rp.site_maps():
            if sitemap not in self.sitemap:
                self.sitemap.append(sitemap)

        self.load_sitemap()


    def load_sitemap(self, map_url=[]):

        headers = {'User-Agent': self.ua}

        if not map_url:
            map_url = self.sitemap

        for sitemapurl in map_url:

            sitemap_raw = self.get_req(sitemapurl, headers=headers)
            soup = bs(sitemap_raw, 'lxml')

            for link in soup.find_all('loc'):
                if link.text not in self.in_links:
                    self.in_links.append(link.text)


    def crawl(self):

        # Processes take time, so figure out the time when the next request should be
        # while respecting the rate limit / crawl delay, but count process time as part
        # of the wait. In other words, process while waiting,  don't process and then
        # wait the crawl delay.
        # TODO: move this into some kind of function
        sleep_for = self.rp.crawl_delay(self.ua)
        sleep_for = 2 if sleep_for is None else sleep_for
        next_req = time.time() + sleep_for

        logging.info('Crawling site %s with %s second delay between requests', self.host, sleep_for)

        if not self.in_links:
            base_url = f'https://{self.host}/'
            if not self.obey_robots or self.rp.can_fetch(self.ua, base_url):
                self.extract_links(base_url)

            wait = next_req - time.time()
            if wait > 0:
                time.sleep(wait)

            next_req = time.time() + sleep_for

        idx = -1
        while idx < len(self.in_links):

            idx += 1

            try:
                crawl_url = self.in_links[idx]
            except:
                logging.info('Crawling complete')
                return

            if self.obey_robots and not self.rp.can_fetch(self.ua, crawl_url):
                logging.info('URL fetch prevented by robots.txt: %s', crawl_url)
                continue

            if self.in_links[idx] in self.visited:
                logging.debug('Skipping visited URL: %s', self.in_links[idx])
                continue

            self.extract_links(self.in_links[idx])

            wait = next_req - time.time()
            if wait > 0:
                time.sleep(wait)

            next_req = time.time() + sleep_for


    def extract_links(self, target_url):

        logging.info('Extracting links from: %s', target_url)

        resp = self.get_req(target_url)
        if resp is None:
            return

        # Add support for text/xml
        if 'text/html' not in resp.headers.get('content-type'):
            logging.info('Skipping invalid MIME type: %s', resp.headers.get('content-type'))
            return

        raw_html = resp.read()
        filename = hashlib.file_digest(io.BytesIO(raw_html), 'sha256').hexdigest() + '.html'

        if self.gzip:
            filename += '.gz'
        fullpath = Path(self.files, filename)

        logging.debug('HTML Size: %s', len(raw_html))

        self.visited[target_url] = {
                'last_visit': time.time(),
                'links': {},
                'file_loc': str(fullpath),
                'malformed': [],
                }

        if not fullpath.is_file():
            if self.gzip:
                with gzip.open(fullpath, 'wb') as f:
                    f.write(raw_html)
            else:
                with open(fullpath, 'w') as f:
                    f.write(raw_html.decode('utf-8'))
        else:
            logging.info('Not overwriting existing file: %s', fullpath)

        logging.info('Wrote HTML to %s', fullpath)

        soup = bs(raw_html, 'html.parser')

        for link in soup.find_all('a'):

            href = link.get('href')
            if href is None:
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
                href = f'https://{self.host}' + href[2:]

            try:
                parts = urlparse(href)
            except:
                logging.info('Failed parsing HREF: %s', href)
                continue

            if not all([parts.scheme, parts.netloc]):
                logging.info('Malformed HREF: %s', href)
                self.visited[target_url]['malformed'].append(href)
                continue

            logging.debug('HREF PARSED: %s', parts)

            if parts.fragment:
                href = href.replace('#'+parts.fragment, '')

            hn = parts.hostname

            # Standardize href - No query strings or fragments
            # We only want real pages, not search results to real pages
            clean_href = 'https://'
            clean_href += hn if hn else self.host
            clean_href += parts.path if parts.path else '/'

            self.visited[target_url]['links'].setdefault(href, clean_href)

            # The rest could all be extracted "offline" from self.visited if necessary
            if hn and self.host not in hn:
                if hn not in self.ex_hosts:
                    logging.info('Found new external host: %s', hn)
                    self.ex_hosts.append(hn)

                if clean_href not in self.ex_links:
                    logging.info('Appending external link: %s', clean_href)
                    self.ex_links.append(clean_href)
            else:
                if clean_href not in self.in_links:
                    logging.info('Appending internal link: %s', clean_href)
                    self.in_links.append(clean_href)


    def save_session(self, filename=None):

        if filename is None:
            filename = self.host.replace('.', '_') + '.json'

        data = {
                'host': self.host,
                'external_hosts': self.ex_hosts,
                'internal_links': self.in_links,
                'external_links': self.ex_links,
                'visited': self.visited,
                }

        json_data = json.dumps(data, indent=4)

        with open(filename, 'w') as f:
            f.write(json_data)

        logging.info('Session data saved to %s', filename)


    def load_session(self, filename=None):

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
                'external_hosts': 'ex_hosts',
                'internal_links': 'in_links',
                'external_links': 'ex_links',
                'visited': 'visited',
                }

        for key, attr in lookup.items():

            try:
                setattr(self, attr, data[key])
            except Exception as exc:
                logging.debug('Skipping key %s: %s', exc)
                continue

        logging.info(
                'Session loaded: Host %s :: %s internal links, %s external links from %s external hosts',
                self.host,
                len(self.in_links),
                len(self.ex_links),
                len(self.ex_hosts)
                )


def parse_args():

    parser = argparse.ArgumentParser(description='Web Crawler')
    parser.add_argument('-s', '--site', dest='site', action='store', help='Crawls given site')
    parser.add_argument('-p', '--page', dest='page', action='store', help='Crawls given page')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', default=False, help='Show debug output')
    parser.add_argument('-z', '--gzip', dest='gzip', action='store_true', default=False, help='Store compressed HTML files')

    parsed = parser.parse_args()

    return parsed


if __name__ == '__main__':

    conf = parse_args()

    loglevel = logging.DEBUG if conf.debug else logging.INFO
    logging.basicConfig(level=loglevel, format='%(asctime)s [%(levelname)s] %(message)s')

    if conf.page:
        url_parts = urlparse(conf.page)
        host = url_parts.hostname
        if not host:
            host = url.netloc

        crawler = CrawlSite(host=host, gzip_files=conf.gzip)
        crawler.load_session()
        crawler.load_robots()
        crawler.extract_links(conf.page)
        crawler.save_session()

    if conf.site:
        crawler = CrawlSite(host=conf.site, gzip_files=conf.gzip)
        crawler.load_session()
        crawler.load_robots()
        crawler.crawl()
        crawler.save_session()

