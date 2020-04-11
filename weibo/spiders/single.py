import json
from scrapy import Request, Spider
from weibo.items import *
import os


class SingleSpider(Spider):
    """
    comment spider of single weibo
    """
    name = 'single'
    allowed_domains = ['m.weibo.cn']
    start_url = 'https://m.weibo.cn/comments/hotflow?id={weibo_id}&mid={weibo_id}&max_id_type=0'
    next_url = 'https://m.weibo.cn/comments/hotflow?id={weibo_id}&mid={weibo_id}&max_id_type=1'
    weibo_id = os.getenv('WEIBO_ID', '4467107636950632')
    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'SCHEDULER': None,
        'DUPEFILTER_CLASS': None,
        'REDIS_URL': None,
        'SCHEDULER_PERSIST': False,
        'REDIS_START_URLS_BATCH_SIZE': 5,
        'SCHEDULER_QUEUE_KEY': 'weibo:%(spider)s:requests'
    }
    
    def start_requests(self):
        """
        start from defined users
        :return:
        """
        url = self.start_url.format(weibo_id=self.weibo_id)
        yield Request(url, callback=self.parse_comments, dont_filter=True, priority=10)
    
    def parse_comments(self, response):
        """
        parse comments
        :param response:
        :return:
        """
        result = json.loads(response.text)
        if result.get('ok'):
            data = result.get('data', {})
            comments = data.get('data')
            max_id = data.get('max_id')
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
                    yield comment_item
            
            # next page
            url = self.next_url.format(weibo_id=self.weibo_id)
            yield Request(url, callback=self.parse_comments, dont_filter=True, priority=10)
