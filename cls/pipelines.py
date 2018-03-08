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
from lxml import html
import requests



class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]
        uuids_collection = list(self.collection.find({"post_status": "new"}, { "uuid": 1 }))
        if len(uuids_collection) > 0 : self.list_of_uuids = list(map(lambda x: x['uuid'], uuids_collection))
        log.msg("MongoDBPipeline has been initialize!")
        
    def process_item(self, item, spider):
        valid = True
        for data in item:
            
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))

        
        if self.list_of_uuids:
            if self.is_unique(item) is False:
                valid = False
                raise DropItem("Scraped post already exists with no changes: %s"  % data)

        if valid:
            self.collection.insert(dict(item))
            log.msg("Post added to database!",
                    level=log.DEBUG, spider=spider)
        return item

    
    def is_unique(self, attribute):
        
        if attribute["uuid"] in self.list_of_uuids:
            # code.interact(local=dict(globals(), **locals()))
            return False
            # if post["uuid"] in 
            # for index, post in enumerate(self.posts_with_status_new):
                # if post["uuid"] == attribute["uuid"]:
                
                    # del self.posts_with_status_new[index]

    # def close_spider(self,spider):
        # if len(self.posts_with_status_new) > 0:
        #     for post in self.posts_with_status_new:
                
        #         page = requests.get(post['url'])
        #         tree = html.fromstring(page.content)
        #         if len(tree.xpath('//div[@class="post-not-found"]/h1/text()'))  == 1:
        #             self.collection.update({ "uuid" : post['uuid']}, { "$set": { "post_status": "not-found", "status_changed": "new Date()"}})
                
                    

        
         # print('Closing {} spider'.format(spider.name))

        # get_response(self)
        # self.get_response(self)
        # return scrapy.Request("http://www.example.com/some_page.html",
                          # get_response)
        # yield Request(self.posts_with_status_new[1]['url'], get_response, meta={'URL': absolute_url, 'Title': title, 'Address':address})


