import logging
from functools import lru_cache
from crawler.proxy import ProxyManager
import re
from urllib.parse import urlparse,urlunparse

TIMEOUT=60
PROXY_MANAGER = ProxyManager()
LOGGER = logging.getLogger(__name__)

def clean_url(url):

    parsed = urlparse(url)

    # add scheme if not available
    if not parsed.scheme:
        parsed = parsed._replace(scheme="http")

        url = urlunparse(parsed)

    # clean text anchor from urls if available
    pattern = r'(.+)(\/#[a-zA-Z0-9]+)$'
    m = re.match(pattern, url)

    if m:
        return m.group(1)
    else:
        # clean trailing slash if available
        pattern = r'(.+)(\/)$'
        m = re.match(pattern, url)

        if m:
            return m.group(1)

    return url


def get_content_type(response):
    content_type = response.headers.get("content-type")
    if content_type:
        return content_type.split(';')[0]


@lru_cache(maxsize=8192)
def call(session, url, use_proxy=False, retries=0):
    if use_proxy:
        return _call_with_proxy(session, url, retries)
    else:
        return _call_without_proxy(session, url, retries)


def _call_with_proxy(session, url, retries=0):
    LOGGER.info('_call_with_proxy')
    proxy = PROXY_MANAGER.get_proxy()
    if proxy[0]:
        try:
            response = session.get(url, timeout=TIMEOUT, proxies=proxy[0])
            response.raise_for_status()
        except Exception as e:
            LOGGER.warning('_call_with_proxy error: e=%s', str(e))
            if retries <= 3:
                PROXY_MANAGER.change_proxy(proxy[1])
                return _call_with_proxy(session, url, retries + 1)
            else:
                return None
        else:
            return response
    else:
        return None

def _call_without_proxy(session, url, retries=0):
    LOGGER.info('_call_without_proxy')
    try:
        response = session.get(url, timeout=TIMEOUT)
        LOGGER.info('_call_without_proxy: try: response=%s', str(response))
        response.raise_for_status()
    except Exception as e:
        LOGGER.warning('_call_without_proxy error: e=%s', str(e))
        if retries <= 3:
            return _call_without_proxy(session,url,retries + 1)
        else:
            return None
    else:
        LOGGER.info('_call_without_proxy: else: response=%s', str(response))
        return response
