# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html


# class ClsPipeline(object):
#     def process_item(self, item, spider):
#         return item

from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DropItem
from pymongo import MongoClient
from lxml import html
from scrapy import signals
import logging
import requests
import datetime
from time import sleep

SETTINGS = get_project_settings()
logger = logging.getLogger(__name__)

class MongoDBPipeline(object):

    def __init__(self, stats, settings):
        self.stats = stats
        connection = MongoClient(
            settings['MONGO_URI']
           )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]
        self.collection_stats = db[settings['MONGODB_STATS']]
        
        # retrieve records only with new post status and attibute of uuid
        uuids_collection = list(self.collection.find({"post_status": "new"}, { "uuid": 1 }))
        if len(uuids_collection) > 0 : self.list_of_uuids = list(map(lambda x: x['uuid'], uuids_collection))
        logger.debug("MongoDBPipeline has been initialize!")

    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats,crawler.settings)

    def save_crawl_stats(self):
        tmp_start_time = self.stats.get_value('start_time').strftime("%Y-%m-%d %I:%M:%S%p")
        tmp_end_time = self.stats.get_value('finish_time').strftime("%Y-%m-%d %I:%M:%S%p")
        self.stats.set_value('start_time', tmp_start_time)
        self.stats.set_value('finish_time', tmp_end_time)
        self.stats.set_value('created_at', datetime.datetime.now().strftime("%Y-%m-%d %I:%M:%S%p"))

        self.collection_stats.insert_one(self.stats.get_stats()).inserted_id

    def process_item(self, item, spider):
        valid = True
        for data in item:
            
            if not data:
                valid = False
                raise DropItem("Missing {0}!".format(data))

        if hasattr(self, 'list_of_uuids'):
            if self.is_unique(item) is False:
                valid = False
                raise DropItem("Scraped post already exists with no changes: %s"  % data)

        if valid:
            self.collection.insert(dict(item))
            logger.debug("Post added to database!", dict(item))
        return item

    
    def is_unique(self, attribute):
        uuids_collection = self.list_of_uuids
        if attribute["uuid"] in uuids_collection:
            self.list_of_uuids.remove(attribute["uuid"])
            return False

    def close_spider(self,spider):
        if len(self.list_of_uuids) > 0:
            self.stats.set_value('records_not_found',0)
            self.stats.set_value('records_existing', 0)
            self.stats.set_value('records_flagged', 0)
            self.stats.set_value('records_expired', 0)
            self.stats.set_value('records_deleted', 0)

            for i in self.list_of_uuids:
                post = self.collection.find_one({"uuid": i})
                # requests gets max entries
                sleep(0.5)
                page = requests.get(post['url'])
                tree = html.fromstring(page.content)
                if page.status_code == 404:
                    self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "not-found"}})
                    self.stats.inc_value('records_not_found')
                elif page.status_code == 200:
                    if len(tree.xpath('//section[@class="body"]')) > 0:
                        if len(tree.xpath('//div[@class="postinginfos"]/p[2]/time/text()')) > 0:
                            self.stats.inc_value('records_existing')
                            continue
                        elif len(tree.xpath('//div[@class="removed"]')) > 0:
                            if "expired" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "expired"}})
                                self.stats.inc_value('records_expired')
                            elif "flagged" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "flagged"}})
                                self.stats.inc_value('records_flagged')
                            elif "deleted" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "deleted"}})
                                self.stats.inc_value('records_deleted')
                            else:    
                                from pdb import set_trace; set_trace()
                        else:
                            from pdb import set_trace; set_trace()
                    else:
                        from pdb import set_trace; set_trace()
                else:
                    from pdb import set_trace; set_trace()

        
        print('Closing {} spider'.format(spider.name))




        # get_response(self)
        # self.get_response(self)
        # return scrapy.Request("http://www.example.com/some_page.html",
                          # get_response)
        # yield Request(self.posts_with_status_new[1]['url'], get_response, meta={'URL': absolute_url, 'Title': title, 'Address':address})


