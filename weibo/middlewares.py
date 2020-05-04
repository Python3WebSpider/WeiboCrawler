# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
import logging
import requests
import json
import re

from scrapy.downloadermiddlewares.retry import RetryMiddleware

logger = logging.getLogger(__name__)


class ProxytunnelMiddleware(object):
    def __init__(self, proxytunnel_url):
        self.logger = logger
        self.proxytunnel_url = proxytunnel_url
    
    def process_request(self, request, spider):
        """
        if retry_times > 0，get random proxy
        :param request:
        :param spider:
        :return:
        """
        if request.meta.get('retry_times'):
            self.logger.debug('Using proxytunnel')
            request.meta['proxy'] = self.proxytunnel_url
    
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        return cls(
            proxytunnel_url=settings.get('PROXYTUNNEL_URL')
        )


class CSRFTokenMiddleware(object):
    
    def process_request(self, request, spider):
        pass


class RetryCommentMiddleware(RetryMiddleware):
    
    def process_response(self, request, response, spider):
        try:
            result = json.loads(response.text)
            if not result.get('ok') == 1:
                logger.info('Retrying times %s', request.meta.get('retry_times', 0))
                return self._retry(request, 'Status not OK', spider) or response
            return response
        except json.decoder.JSONDecodeError:
            logger.info('Json decode error, content %s', response.text)
            return self._retry(request, 'Json Decode Error', spider) or response


class ProxypoolMiddleware(object):
    """
    proxy middleware for changing proxy
    """
    
    def __init__(self, proxypool_url):
        self.logger = logging.getLogger(__name__)
        if re.search('^https?://\S+:\S+@\S+', proxypool_url):
            result = re.search('https?://(\S+):(\S+)@\S+', proxypool_url)
            self.auth = result.group(1), result.group(2)
            self.proxypool_url = re.sub('(https?://)\S+:\S+@(\S+)', r'\1\2', proxypool_url)
        else:
            self.proxypool_url = proxypool_url
    
    def get_random_proxy(self):
        """
        get random proxy form proxypol
        :return:
        """
        try:
            if self.auth:
                response = requests.get(self.proxypool_url, timeout=5, auth=self.auth)
            else:
                response = requests.get(self.proxypool_url, timeout=5)
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
            proxypool_url=settings.get('PROXYPOOL_URL')
        )
