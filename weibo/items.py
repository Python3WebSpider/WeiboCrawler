# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy import Item, Field


class UserItem(Item):
    collection = 'users'
    index = 'weibo-users'
    type = 'user'
    
    id = Field()
    name = Field()
    avatar = Field()
    cover = Field()
    gender = Field()
    description = Field()
    fans_count = Field()
    follows_count = Field()
    weibos_count = Field()
    verified = Field()
    verified_reason = Field()
    verified_type = Field()
    follows = Field()
    fans = Field()
    crawled_at = Field()


class WeiboItem(Item):
    collection = 'weibos'
    index = 'weibo-weibos'
    type = 'weibo'
    id = Field()
    attitudes_count = Field()
    comments_count = Field()
    reposts_count = Field()
    picture = Field()
    pictures = Field()
    source = Field()
    text = Field()
    raw_text = Field()
    thumbnail = Field()
    user = Field()
    user_name = Field()
    created_at = Field()
    crawled_at = Field()


class CommentItem(Item):
    collection = 'comments'
    index = 'weibo-comments'
    type = 'comment'
    
    id = Field()
    likes_count = Field()
    source = Field()
    text = Field()
    raw_text = Field()
    user = Field()
    created_at = Field()
    reply_id = Field()
    reply_text = Field()
    reply_raw_text = Field()
    weibo = Field()
    crawled_at = Field()
