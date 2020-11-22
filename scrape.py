import requests
from urllib.parse import urlparse
from bs4 import BeautifulSoup

# global user-agent
UserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"

class Scraper(object):
    pass

class RemotePage(object):

    _url = None

    # parse
    _soup = None

    # parse out after fetch
    _content = None
    _title = None
    _imgs = None

    # config
    _img_tags = ['img', 'picture']
    _img_url_prop = ['src']

    def __init__(self, url):
        # xx/
        self._url = url if not url.endswith('/') else url[0:-1]
        # add http://
        self._url = self._url if self._url.find('://') > 0 else 'http://' + self._url
        self._uri = urlparse(self._url)

        self._fetch()

    def _fetch(self, force=False):
        try:
            resp = requests.get(self._url, headers={'User-Agent': UserAgent}, timeout=10)
            if resp.status_code != 200:
                pass
            self._content = resp.content.decode(resp.apparent_encoding)
        except:
            pass

    def _normalize_url(self, url):
        # http://, https://, //, /xxx, xxxx,
        if url.startswith('//'): return 'http:' + url
        return url if url.startswith('http') or url.startswith('data:image') else "{0}{1}{2}".format(self.url, '' if url.startswith('/') else '/', url)

    @property
    def soup(self):
        if self._soup is None:
            self._soup = BeautifulSoup(self.content, 'html.parser')
        return self._soup

    @property
    def url(self):
        return self._url

    @property
    def title(self):
        if self._title is None:
            if self.soup.title:
                self._title = self.soup.title.string
        return self._title

    @property
    def images(self):
        # return all images
        if self._imgs is not None:
            return self._imgs

        self._imgs = []

        for tag in self._img_tags:
            for r in self.soup.find_all(tag):
                for prop in self._img_url_prop:
                    _img_url = r.get(prop)
                    if _img_url:
                        self._imgs.append(self._normalize_url(_img_url))
                        break

        return self._imgs

    @property
    def content(self):
        return self._content if self._content is not None else ''


def __test():
    urls = [
        "https://www.spdb.com.cn",
        "http://cwtc.com",
        # "https://cn.bing.com"
    ]

    for u in urls:
        page = RemotePage(u)
        print("title =>", page.title)
        # print("content =>", page.content)
        print("images =>", page.images)


if __name__ == '__main__':
    __test()
