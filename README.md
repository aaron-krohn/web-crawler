# Web Crawler Alpha

First steps of making a multi-process web scraper. Obeys robots file directives where applicable.

## Usage

Crawls a site or a page, extracts links, saves link data to a JSON file, and HTML pages to disk.

Press Ctrl+C to interrupt crawl and save session to JSON before exit. Only press it once though, if you press twice quickly, you might interrupt the file write process.

```
$ python3.11 crawl.py --help
usage: crawl.py [-h] [-s SITE] [-p PAGE] [-d] [-z] [-n] [-c CACHELIMIT] [-l LOGFILE]

Web Crawler

options:
  -h, --help            show this help message and exit
  -s SITE, --site SITE  Crawls given site
  -p PAGE, --page PAGE  Crawls given page
  -d, --debug           Show debug output
  -z, --gzip            Store compressed HTML files
  -n, --no-cache        Refresh cached pages
  -c CACHELIMIT, --cache-limt CACHELIMIT
                        Cache expiration limit
  -l LOGFILE, --log LOGFILE
                        File location for log file
```
