# Web Crawler Alpha

First steps of making a multi-process web scraper

## Usage

Crawls a site or a page, extracts links, saves link data to a JSON file, and HTML pages to disk.

```
$ python3.11 crawl.py --help
usage: crawl.py [-h] [-s SITE] [-p PAGE] [-d] [-z]

Web Crawler

options:
  -h, --help            show this help message and exit
  -s SITE, --site SITE  Crawls given site
  -p PAGE, --page PAGE  Crawls given page
  -d, --debug           Show debug output
  -z, --gzip            Store compressed HTML files
```
