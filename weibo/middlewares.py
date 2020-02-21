# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import logging
import requests


class ProxyMiddleware(object):
    """
    proxy middleware for changing proxy
    """
    
    def __init__(self, proxy_url):
        self.logger = logging.getLogger(__name__)
        self.proxy_url = proxy_url
    
    def get_random_proxy(self):
        """
        get random proxy form proxypol
        :return:
        """
        try:
            response = requests.get(self.proxy_url, timeout=5)
            if response.status_code == 200:
                proxy = response.text
                return proxy
        except requests.ConnectionError:
            return False
    
    def process_request(self, request, spider):
        """
        if retry_times > 0，get random proxy
        :param request:
        :param spider:
        :return:
        """
        if request.meta.get('retry_times'):
            proxy = self.get_random_proxy()
            self.logger.debug('Get proxy %s', proxy)
            if proxy:
                uri = 'http://{proxy}'.format(proxy=proxy)
                self.logger.debug('Using proxy %s', proxy)
                request.meta['proxy'] = uri
    
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            proxy_url=settings.get('PROXY_URL')
        )
