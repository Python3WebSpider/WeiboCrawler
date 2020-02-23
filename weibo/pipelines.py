# -*- coding: utf-8 -*-

import re, time
from datetime import datetime
from elasticsearch import Elasticsearch
from scrapy import Selector
import pymongo
from weibo.items import *
from twisted.internet.threads import deferToThread
import pytz

class TimePipeline():
    """
    time pipeline
    """
    
    def process_item(self, item, spider):
        """
        add crawled_at attr
        :param item:
        :param spider:
        :return:
        """
        if isinstance(item, UserItem) or isinstance(item, WeiboItem) or isinstance(item, CommentItem):
            item['crawled_at'] = datetime.now(tz=pytz.utc)
        return item


class WeiboPipeline():
    """
    weibo pipeline
    """
    
    def parse_time(self, date):
        """
        parse weibo time
        :param date:
        :return:
        """
        if re.match('刚刚', date):
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        if re.match('\d+分钟前', date):
            minute = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - float(minute) * 60))
        if re.match('\d+小时前', date):
            hour = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - float(hour) * 60 * 60))
        if re.match('昨天.*', date):
            date = re.match('昨天(.*)', date).group(1).strip()
            date = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + ' ' + date + ':00'
        if re.match('\d{2}-\d{2}', date):
            date = time.strftime('%Y-', time.localtime()) + date + ' 00:00:00'
        return date
    
    def process_item(self, item, spider):
        """
        process weibo item
        :param item:
        :param spider:
        :return:
        """
        if isinstance(item, WeiboItem):
            if item.get('created_at'):
                item['created_at'] = item['created_at'].strip()
                item['created_at'] = self.parse_time(item.get('created_at'))
            if item.get('pictures'):
                item['pictures'] = [pic.get('url') for pic in item.get('pictures')]
            if item.get('text'):
                item['raw_text'] = ''.join(Selector(text=item.get('text')).xpath('//text()').extract())
        return item


class CommentPipeline():
    """
    comment pipeline
    """
    
    def parse_time(self, date):
        """
        parse comment time
        :param date:
        :return:
        """
        if re.match('刚刚', date):
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        if re.match('\d+分钟前', date):
            minute = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - float(minute) * 60))
        if re.match('\d+小时前', date):
            hour = re.match('(\d+)', date).group(1)
            date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time() - float(hour) * 60 * 60))
        if re.match('昨天.*', date):
            date = re.match('昨天(.*)', date).group(1).strip()
            date = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60)) + ' ' + date + ':00'
        if re.match('\d{2}-\d{2}', date):
            date = time.strftime('%Y-', time.localtime()) + date + ' 00:00:00'
        return date
    
    def process_item(self, item, spider):
        """
        process comment item
        :param item:
        :param spider:
        :return:
        """
        if isinstance(item, CommentItem):
            if item.get('user'):
                item['user'] = item.get('user').get('id')
            if item.get('text'):
                item['raw_text'] = ''.join(Selector(text=item.get('text')).xpath('//text()').extract())
                if re.search('回复.*?\:(.*?)', item.get('raw_text')):
                    item['raw_text'] = re.search('回复.*?\:(.*)', item.get('raw_text')).group(1)
            if item.get('reply_text'):
                item['reply_raw_text'] = ''.join(Selector(text=item.get('reply_text')).xpath('//text()').extract())
                if re.search('回复.*?\:(.*?)', item.get('reply_raw_text')):
                    item['reply_raw_text'] = re.search('回复.*?\:(.*)', item.get('reply_raw_text')).group(1)
            if item.get('created_at'):
                item['created_at'] = item['created_at'].strip()
                item['created_at'] = self.parse_time(item.get('created_at'))
        return item


class MongoPipeline(object):
    """
    mongodb pipeline
    """
    
    def __init__(self, mongo_uri, mongo_db):
        """
        init conn
        :param mongo_uri:
        :param mongo_db:
        """
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
    
    @classmethod
    def from_crawler(cls, crawler):
        """
        get settings
        :param crawler:
        :return:
        """
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE')
        )
    
    def open_spider(self, spider):
        """
        create conn while creating spider
        :param spider:
        :return:
        """
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        self.db[UserItem.collection].create_index([('id', pymongo.ASCENDING)])
        self.db[WeiboItem.collection].create_index([('id', pymongo.ASCENDING)])
    
    def close_spider(self, spider):
        """
        close conn
        :param spider:
        :return:
        """
        self.client.close()
    
    def _process_item(self, item, spider):
        """
        main processor
        :param item:
        :param spider:
        :return:
        """
        if isinstance(item, UserItem) or isinstance(item, WeiboItem) or isinstance(item, CommentItem):
            self.db[item.collection].update({'id': item.get('id')}, {'$set': item}, True)
        return item
    
    def process_item(self, item, spider):
        """
        process item using defer
        :param item:
        :param spider:
        :return:
        """
        return deferToThread(self._process_item, item, spider)


class ElasticsearchPipeline(object):
    """
    pipeline for elasticsearch
    """
    
    def __init__(self, connection_string):
        """
        init connection_string and mappings
        :param connection_string:
        """
        self.connection_string = connection_string
    
    @classmethod
    def from_crawler(cls, crawler):
        """
        class method for pipeline
        :param crawler: scrapy crawler
        :return:
        """
        return cls(
            connection_string=crawler.settings.get('ELASTICSEARCH_CONNECTION_STRING'),
        )
    
    def open_spider(self, spider):
        """
        open spider to do
        :param spider:
        :return:
        """
        self.conn = Elasticsearch(
            hosts=[self.connection_string],
            use_ssl=False,
            verify_certs=False
        )
    
    def _process_item(self, item, spider):
        """
        main process
        :param item: user or weibo or comment item
        :param spider:
        :return:
        """
        if isinstance(item, UserItem) or isinstance(item, WeiboItem) or isinstance(item, CommentItem):
            self.conn.index(index=item.index, id=item['id'], doc_type=item.type, body=dict(item))
        return item
    
    def process_item(self, item, spider):
        """
        process item using deferToThread
        :param item:
        :param spider:
        :return:
        """
        return deferToThread(self._process_item, item, spider)
