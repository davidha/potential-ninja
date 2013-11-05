import HTMLParser
import Queue
import StringIO
import getopt
import gzip
import sys
import threading
import urllib2
import urlparse

import bs4


class NormalizedUrlHTTPRedirectHandler(urllib2.HTTPRedirectHandler):
    """Normalize URL before redirecting"""

    def redirect_request(self, req, fp, code, msg, hdrs, newurl):
        return urllib2.HTTPRedirectHandler.redirect_request(
            self,
            req,
            fp,
            code,
            msg,
            hdrs,
            normalize_url(newurl)
        )


class PageInfo:
    def __init__(self, url):
        self.url = url
        self.static_assets = []
        self.redirected = False
        self.failed = False
        self.error_msg = ""

    def mark_as_redirected(self):
        self.redirected = True

    def mark_as_failed(self, error_msg):
        self.failed = True
        self.error_msg = error_msg


class Crawler(threading.Thread):
    opener = urllib2.build_opener(NormalizedUrlHTTPRedirectHandler)

    def __init__(self, url_queue, url_allowable,
                 visited_pages, visited_pages_lock):
        super(Crawler, self).__init__()
        super(Crawler, self).setDaemon(True)

        self.url_queue = url_queue
        self.url_allowable = url_allowable
        self.visited_pages = visited_pages
        self.visited_pages_lock = visited_pages_lock

    def run(self):
        """Process a URL from the queue if selection policy allows it,
        and provided it's not already been visited"""

        while True:
            url = self.url_queue.get()
            url = normalize_url(url)

            if not self.url_allowable(url):
                self.url_queue.task_done()
                continue

            with self.visited_pages_lock:
                if url not in self.visited_pages:
                    already_seen = False
                    new_page_info = PageInfo(url)
                    self.visited_pages[url] = new_page_info
                else:
                    already_seen = True

            if not already_seen:
                self.visit_page(url, new_page_info)

            self.url_queue.task_done()

    def visit_page(self, url, page_info):
        request = urllib2.Request(url)
        request.add_header("User-Agent", "David's Crawler")
        request.add_header("Accept-Encoding", "gzip")

        try:
            response = self.opener.open(request)
        except urllib2.URLError as err:
            page_info.mark_as_failed(err.msg)
            return

        if response.geturl() != url:
            # keep old entry in dictionary, because we have visited it
            page_info.mark_as_redirected()

            new_url = response.geturl()
            if self.url_allowable(new_url):
                with self.visited_pages_lock:
                    if new_url not in self.visited_pages:
                        url = new_url
                        page_info = PageInfo(url)
                        self.visited_pages[url] = page_info
                    else:
                        # nothing to do, already processed
                        return
            else:
                # can't process page
                return

        try:
            f = None
            buf = None

            if response.info().getheader("Content-Encoding") == "gzip":
                buf = StringIO.StringIO(response.read())
                f = gzip.GzipFile(fileobj=buf)
            else:
                f = response

            page_links, page_static_assets = self.parse_page(f, url)
        except (HTMLParser.HTMLParseError, IOError) as err:
            page_info.mark_as_failed(err.msg)
            return
        finally:
            if f is not None:
                f.close()
            if buf is not None:
                buf.close()

        page_info.static_assets = page_static_assets

        for pl in page_links:
            self.url_queue.put(pl)

    def parse_page(self, f, base_url):
        soup = bs4.BeautifulSoup(f)

        a_tags = soup.find_all("a", href=True)
        page_links = \
            [make_absolute_url(base_url, at["href"].strip()) for at in a_tags]

        tags = soup.find_all(["img", "script"], src=True)
        static_assets = \
            [make_absolute_url(base_url, tag["src"].strip()) for tag in tags]
        tags = soup.find_all("link", rel="stylesheet", href=True)
        static_assets.extend(
            [make_absolute_url(base_url, tag["href"].strip()) for tag in tags])

        return page_links, set(static_assets)


def crawl_domain(start_url, num_crawlers):
    """Start crawling start_url with num_crawlers crawlers, keeping within
    start_url's domain"""

    start_url_domain = urlparse.urlparse(start_url).hostname
    def domain_limited_http_url_allowable(url):
        parsed_url = urlparse.urlparse(url)
        return (parsed_url.hostname == start_url_domain and
                parsed_url.scheme in ("http", "https"))

    # maintain a shared queue of URLs that need to be crawled
    url_queue = Queue.Queue()
    # maintain a shared dictionary of results, keyed against URL
    visited_pages = {}
    visited_pages_lock = threading.Lock()

    for i in range(num_crawlers):
        crawler = Crawler(url_queue, domain_limited_http_url_allowable,
                          visited_pages, visited_pages_lock)
        crawler.start()

    url_queue.put(start_url)
    url_queue.join()

    sorted_urls = sorted(visited_pages.iterkeys())
    for url in sorted_urls:
        page_info = visited_pages[url]
        if not page_info.redirected:
            print page_info.url
            if not page_info.failed:
                for static_asset in page_info.static_assets:
                    print "\t%s" % (static_asset,)
            else:
                print "\terror loading page: %s" % (page_info.error_msg)


def make_absolute_url(base_url, url):
    return urlparse.urljoin(base_url, url)


def normalize_url(url):
    parsed_url = urlparse.urlparse(url)

    if parsed_url.username is not None:
        if parsed_url.password is not None:
            auth = "%s:%s@" % (parsed_url.username, parsed_url.password)
        else:
            auth = "%s@" % (parsed_url.username,)
    else:
        auth = ""

    hostname = parsed_url.hostname if parsed_url.hostname is not None else ""

    port = parsed_url.port
    if port == 80 and parsed_url.scheme == "http":
        port = None
    elif port == 443 and parsed_url.scheme == "https":
        port = None

    if port is not None:
        new_netloc = "%s%s:%d" % (auth, hostname, port)
    else:
        new_netloc = "%s%s" % (auth, hostname)

    path = parsed_url.path if parsed_url.path else "/"

    new_url_tuple = (parsed_url.scheme,
                     new_netloc,
                     path,
                     parsed_url.params,
                     "",
                     "")

    return urlparse.urlunparse(new_url_tuple)


def usage():
    print "usage: webcrawler.py [-t NUM_THREADS] URL_TO_CRAWL"


if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:h", ["help"])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    num_threads = 1

    for o, v in opts:
        if o == "-t":
            try:
                num_threads = int(v)
            except ValueError:
                print "NUM_THREADS must be an int"
                usage()
                sys.exit(2)
        elif o in ("-h", "--help"):
            usage()
            sys.exit()

    if len(args) < 1:
        usage()
        sys.exit(2)

    start_url = args[0]

    crawl_domain(start_url, num_threads)

