import json
from scrapy import Request, Spider
from weibo.items import *
from urllib.parse import urlparse, parse_qs
import scrapy_redis


class CommentSpider(Spider):
    """
    universal spider to crawl all weibo
    """
    name = 'comment'
    allowed_domains = ['m.weibo.cn']
    user_url = 'https://m.weibo.cn/api/container/getIndex?uid={uid}&type=uid&value={uid}&containerid=100505{uid}'
    follow_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_followers_-_{uid}&page={page}'
    fan_url = 'https://m.weibo.cn/api/container/getIndex?containerid=231051_-_fans_-_{uid}&page={page}'
    weibo_url = 'https://m.weibo.cn/api/container/getIndex?uid={uid}&type=uid&page={page}&containerid=107603{uid}'
    comment_url = 'https://m.weibo.cn/api/comments/show?id={id}&page={page}'
    start_users = ['1195230310', '3217179555', '1742566624', '2282991915', '1288739185', '3952070245', '5878659096']
    
    def start_requests(self):
        """
        start from defined users
        :return:
        """
        for uid in self.start_users:
            yield Request(self.user_url.format(uid=uid), callback=self.parse_user, dont_filter=True)
    
    def parse_user(self, response):
        """
        parse user info
        :param response: user response
        """
        self.logger.debug(response)
        result = json.loads(response.text)
        if result.get('data').get('userInfo'):
            user_info = result.get('data').get('userInfo')
            user_item = UserItem()
            field_map = {
                'id': 'id', 'name': 'screen_name', 'avatar': 'profile_image_url', 'cover': 'cover_image_phone',
                'gender': 'gender', 'description': 'description', 'fans_count': 'followers_count',
                'follows_count': 'follow_count', 'weibos_count': 'statuses_count', 'verified': 'verified',
                'verified_reason': 'verified_reason', 'verified_type': 'verified_type'
            }
            for field, attr in field_map.items():
                user_item[field] = user_info.get(attr)
            yield user_item
            uid = user_info.get('id')
            # follows
            yield Request(self.follow_url.format(uid=uid, page=1), callback=self.parse_follows,
                          meta={'page': 1, 'uid': uid}, dont_filter=True)
            # fans
            yield Request(self.fan_url.format(uid=uid, page=1), callback=self.parse_fans,
                          meta={'page': 1, 'uid': uid}, dont_filter=True)
            # weibos
            yield Request(self.weibo_url.format(uid=uid, page=1), callback=self.parse_weibos,
                          meta={'page': 1, 'uid': uid, 'name': user_item['name']}, dont_filter=True, priority=5)
    
    def parse_follows(self, response):
        """
        parse follows
        :param response: follows response
        """
        result = json.loads(response.text)
        if result.get('ok') and result.get('data').get('cards') and len(result.get('data').get('cards')) and \
                result.get('data').get('cards')[-1].get(
                    'card_group'):
            # parse users
            follows = result.get('data').get('cards')[-1].get('card_group')
            for follow in follows:
                if follow.get('user'):
                    uid = follow.get('user').get('id')
                    yield Request(self.user_url.format(uid=uid), callback=self.parse_user, dont_filter=True)
            uid = response.meta.get('uid')
            
            # next page
            page = response.meta.get('page') + 1
            yield Request(self.follow_url.format(uid=uid, page=page),
                          callback=self.parse_follows, meta={'page': page, 'uid': uid}, dont_filter=True)
    
    def parse_fans(self, response):
        """
        parse fans
        :param response: fans response
        """
        result = json.loads(response.text)
        if result.get('ok') and result.get('data').get('cards') and len(result.get('data').get('cards')) and \
                result.get('data').get('cards')[-1].get(
                    'card_group'):
            # parse users
            fans = result.get('data').get('cards')[-1].get('card_group')
            for fan in fans:
                if fan.get('user'):
                    uid = fan.get('user').get('id')
                    yield Request(self.user_url.format(uid=uid), callback=self.parse_user, dont_filter=True)
            
            uid = response.meta.get('uid')
            # next page
            page = response.meta.get('page') + 1
            yield Request(self.fan_url.format(uid=uid, page=page),
                          callback=self.parse_fans, meta={'page': page, 'uid': uid}, dont_filter=True)
    
    def parse_weibos(self, response):
        """
        parse weibos
        :param response: weibos response
        """
        result = json.loads(response.text)
        if result.get('ok') and result.get('data').get('cards'):
            weibos = result.get('data').get('cards')
            for weibo in weibos:
                mblog = weibo.get('mblog')
                if mblog:
                    weibo_item = WeiboItem()
                    field_map = {
                        'id': 'id', 'attitudes_count': 'attitudes_count', 'comments_count': 'comments_count',
                        'reposts_count': 'reposts_count', 'picture': 'original_pic', 'pictures': 'pics',
                        'created_at': 'created_at', 'source': 'source', 'text': 'text', 'raw_text': 'raw_text',
                        'thumbnail': 'thumbnail_pic',
                    }
                    for field, attr in field_map.items():
                        weibo_item[field] = mblog.get(attr)
                    weibo_item['user'] = response.meta.get('uid')
                    weibo_item['user_name'] = response.meta.get('name')
                    yield weibo_item
                    comment_url = self.comment_url.format(id=weibo_item['id'], page=1)
                    yield Request(comment_url, callback=self.parse_comments, dont_filter=True, priority=10)
            
            # next page
            uid = response.meta.get('uid')
            page = response.meta.get('page') + 1
            yield Request(self.weibo_url.format(uid=uid, page=page), callback=self.parse_weibos,
                          meta={'uid': uid, 'page': page, 'name': response.meta.get('name')}, dont_filter=True,
                          priority=5)
    
    def parse_comments(self, response):
        """
        parse comments
        :param response:
        :return:
        """
        result = json.loads(response.text)
        if result.get('ok'):
            comments = result.get('data', {}).get('data')
            params = parse_qs(urlparse(response.url).query)
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
                    comment_item['weibo'] = params.get('id')[0]
                    yield comment_item
                
                # next page
                page = str(int(params.get('page')[0]) + 1) if params.get('page') else '2'
                yield Request(self.comment_url.format(id=params.get('id')[0], page=page),
                              callback=self.parse_comments, dont_filter=True, priority=10)
