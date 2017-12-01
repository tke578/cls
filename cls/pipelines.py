# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


# class ClsPipeline(object):
#     def process_item(self, item, spider):
#         return item

import pymongo

from scrapy.conf import settings
from scrapy.exceptions import DropItem
from pymongo import MongoClient
from scrapy import log
import code


class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]
        self.posts_with_status_new = list(self.collection.find({"post_status": "new", "uuid":{"$ne":None}}))
        log.msg("MongoDBPipeline has been initialize!")
        
    def process_item(self, item, spider):
        valid = True
        for data in item:
            
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))

        if self.is_unique(item) == False:
            valid = False
            raise DropItem("Scraped post already exists with no changes: %s"  % data)


        if valid:
            self.collection.insert(dict(item))
            log.msg("Post added to database!",
                    level=log.DEBUG, spider=spider)
        return item

    
    def is_unique(self, attribute):
        if len(self.posts_with_status_new) > 0:
            for index, post in enumerate(self.posts_with_status_new):
                if post["uuid"] == attribute["uuid"]:
                    del self.posts_with_status_new[index]
                    return False

    def close_spider(self,spider):
        

