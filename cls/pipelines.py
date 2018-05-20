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
from lxml import html
import logging
import requests



class MongoDBPipeline(object):

    def __init__(self):
        connection = pymongo.MongoClient(
            settings['MONGODB_SERVER'],
            settings['MONGODB_PORT']
        )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]
        # retrieve records only with new post status and attibute of uuid
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
        uuids_collection = self.list_of_uuids
        if attribute["uuid"] in uuids_collection:
            self.list_of_uuids.remove(attribute["uuid"])
            return False

    def close_spider(self,spider):
        if len(self.list_of_uuids) > 0:
            records_not_found = 0
            records_existing = 0
            records_flagged = 0
            records_expired = 0
            records_deleted = 0
            for i in self.list_of_uuids:
                post = self.collection.find_one({"uuid": i})
                page = requests.get(post['url'])
                tree = html.fromstring(page.content)
                if page.status_code == 404:
                    self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "not-found"}})
                    records_not_found += 1
                elif page.status_code == 200:
                    if len(tree.xpath('//section[@class="body"]')) > 0:
                        if len(tree.xpath('//div[@class="postinginfos"]/p[2]/time/text()')) > 0:
                            records_existing += 1
                            continue
                        elif len(tree.xpath('//div[@class="removed"]')) > 0:
                            if "expired" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "expired"}})
                                records_expired += 1
                            elif "flagged" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "flagged"}})
                                records_flagged += 1
                            elif "deleted" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "deleted"}})
                                records_deleted += 1
                            else:    
                                from pdb import set_trace; set_trace()
                        else:
                            from pdb import set_trace; set_trace()
                    else:
                        from pdb import set_trace; set_trace()
                else:
                    from pdb import set_trace; set_trace()

            print('Size of records_not_found ', records_not_found)
            print('Size of records_flagged', records_flagged)
            print('Size of records_existing', records_existing)
            print('Size of records_expired', records_expired)
            print('Size of records_deleted', records_deleted)
        
        print('Closing {} spider'.format(spider.name))

        # get_response(self)
        # self.get_response(self)
        # return scrapy.Request("http://www.example.com/some_page.html",
                          # get_response)
        # yield Request(self.posts_with_status_new[1]['url'], get_response, meta={'URL': absolute_url, 'Title': title, 'Address':address})


