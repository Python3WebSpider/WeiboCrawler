import json
import logging

from scrapy import Request, Spider
from weibo.items import *
import os
from environs import Env
import random

env = Env()


class SingleSpider(Spider):
    """
    comment spider of single weibo
    """
    name = 'single'
    allowed_domains = ['m.weibo.cn']
    start_url = 'https://m.weibo.cn/comments/hotflow?id={weibo_id}&mid={weibo_id}&max_id_type=0'
    next_url = 'https://m.weibo.cn/comments/hotflow?id={weibo_id}&mid={weibo_id}&max_id={max_id}&max_id_type=1'
    weibo_id = os.getenv('WEIBO_WEIBO_ID', '4467107636950632')
    custom_settings = {
        'DOWNLOAD_DELAY': 10,
        'COOKIES_ENABLED': True,
        # 'LOG_LEVEL': 'INFO',
        # 'COOKIES_DEBUG': True,
        'SCHEDULER': 'scrapy.core.scheduler.Scheduler',
        'REDIS_START_URLS_BATCH_SIZE': 5,
        'RETRY_TIMES': 50,
        'SCHEDULER_QUEUE_KEY': 'weibo:%(spider)s:requests' + str(random.randint(0, 100000)).zfill(6),
        'DOWNLOADER_MIDDLEWARES': {
            'weibo.middlewares.CSRFTokenMiddleware': 701,
            'weibo.middlewares.RetryCommentMiddleware': 551,
            'weibo.middlewares.ProxypoolMiddleware': 555 if env.bool('PROXYPOOL_ENABLED', True) else None,
            'weibo.middlewares.ProxytunnelMiddleware': 556 if env.bool('PROXYTUNNEL_ENABLED', True) else None,
        }
    }
    
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://m.weibo.cn/detail/" + weibo_id,
        "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Mobile Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    
    cookies = env.str('WEIBO_COOKIES')
    page = 1
    
    def start_requests(self):
        """
        start from defined users
        :return:
        """
        url = self.start_url.format(weibo_id=self.weibo_id)
        # 赋值初始 Cookies
        cookies = {
            cookies_item.split('=')[0].strip(): cookies_item.split('=')[1].strip() for cookies_item in
            self.cookies.split(';')}
        yield Request(url, headers=self.headers, cookies=cookies, callback=self.parse_comments,
                      priority=10, dont_filter=True, meta={'page': self.page})
    
    def parse_comments(self, response):
        """
        parse comments
        :param response:
        :return:
        """
        page = response.meta['page']
        self.logger.info('Crawled Page %s', page)
        result = json.loads(response.text)
        if result.get('ok'):
            data = result.get('data', {})
            comments = data.get('data')
            max_id = data.get('max_id')
            if not max_id:
                self.logger.error('Cannot get max_id from %s', response.url)
                return
            if comments:
                for comment in comments:
                    comment_item = CommentItem()
                    field_map = {
                        'id': 'id',
                        'likes_count': 'like_counts',
                        'text': 'text',
                        'reply_text': 'reply_text',
                        'created_at': 'created_at',
                        'source': 'source',
                        'user': 'user',
                        'reply_id': 'reply_id',
                    }
                    for field, attr in field_map.items():
                        comment_item[field] = comment.get(attr)
                    comment_item['weibo'] = self.weibo_id
                    self.logger.info('Comment %s %s %s',
                                     comment_item['id'],
                                     comment_item['text'],
                                     comment_item['created_at'])
                    yield comment_item
            else:
                self.logger.error('No Comments Data %s', data)
            # next page
            url = self.next_url.format(weibo_id=self.weibo_id, max_id=max_id)
            self.logger.info('Next url %s', url)
            yield Request(url, headers=self.headers, callback=self.parse_comments, priority=10, dont_filter=True,
                          meta={'page': page + 1})
        else:
            self.logger.error('Result not ok %s', result)
