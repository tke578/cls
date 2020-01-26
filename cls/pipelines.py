
from scrapy.utils.project import get_project_settings
from scrapy.exceptions import DropItem
from pymongo import MongoClient
from lxml import html
from scrapy import signals
import logging
import requests
from datetime import datetime
from time import sleep
from cls.utilities import Slack
from cls.settings import SLACK_CHANNEL

SETTINGS = get_project_settings()
logger = logging.getLogger(__name__)

class MongoDBPipeline(object):

    def __init__(self, stats, settings):
        self.stats = stats
        self.slack_client = Slack()
        connection = MongoClient(
            settings['MONGO_URI']
           )
        db = connection[settings['MONGODB_DB']]
        self.collection = db[settings['MONGODB_COLLECTION']]
        self.collection_stats = db[settings['MONGODB_STATS']]
        
        # retrieve records only with new post status and attibute of uuid
        uuids_collection = list(self.collection.find({"post_status": "new"}, { "uuid": 1 }))
        if len(uuids_collection) > 0: 
            self.list_of_uuids = list(map(lambda x: x['uuid'], uuids_collection))
        logger.debug("MongoDBPipeline has been initialize!")

    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.stats,crawler.settings)

    def save_crawl_stats(self):
        tmp_start_time = self.stats.get_value('start_time').strftime("%Y-%m-%d %I:%M:%S%p")
        tmp_end_time = self.stats.get_value('finish_time').strftime("%Y-%m-%d %I:%M:%S%p")
        self.stats.set_value('start_time', tmp_start_time)
        self.stats.set_value('finish_time', tmp_end_time)
        self.stats.set_value('created_at', datetime.now().strftime("%Y-%m-%d %I:%M:%S%p"))

        self.collection_stats.insert_one(self.stats.get_stats()).inserted_id

    def process_item(self, item, spider):
        #check for missing fields
        valid = True
        for data in item:
            if not data:
                valid = False
                msg = f'Missing {data}!'
                error_msg = self.slack_client.send_message(msg, channel={"name": SLACK_CHANNEL})
                self.slack_client.send_message(f'`{item}`', thread=error_msg['thread'], channel={"name": SLACK_CHANNEL})
                raise DropItem(msg)

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
        #check on previous scraped items
        if len(self.list_of_uuids) > 0:
            self.stats.set_value('records_not_found',0)
            self.stats.set_value('records_existing', 0)
            self.stats.set_value('records_flagged', 0)
            self.stats.set_value('records_expired', 0)
            self.stats.set_value('records_deleted', 0)

            try:
                for i in self.list_of_uuids:
                    post = self.collection.find_one({"uuid": i})
                    # requests gets max entries
                    sleep(0.5)
                    page = requests.get(post['url'])
                    tree = html.fromstring(page.content)
                    if page.status_code == 404:
                        self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "not-found", "updated_at": datetime.now()}})
                        self.stats.inc_value('records_not_found')
                    elif page.status_code == 200:
                        if len(tree.xpath('//section[@class="body"]')) > 0:
                            if len(tree.xpath('//div[@class="postinginfos"]/p[2]/time/text()')) > 0:
                                self.stats.inc_value('records_existing')
                                continue
                            elif len(tree.xpath('//div[@class="removed"]')) > 0:
                                if "expired" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                    self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "expired", "updated_at": datetime.now()}})
                                    self.stats.inc_value('records_expired')
                                elif "flagged" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                    self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "flagged", "updated_at": datetime.now()}})
                                    self.stats.inc_value('records_flagged')
                                elif "deleted" in tree.xpath('//div[@class="removed"]/h2/text()')[0]:
                                    self.collection.update({ "uuid": post['uuid']}, { "$currentDate": { "status_changed": True }, "$set": { "post_status": "deleted", "updated_at": datetime.now()}})
                                    self.stats.inc_value('records_deleted')
                                else:
                                    error_msg = 'Unknown status on a removed post'
                                    error_response = self.slack_client.send_message(error_msg, channel={"name": SLACK_CHANNEL})
                                    self.slack_client.send_message(tree.xpath('//div[@class="removed"]/h2/text()'), thread=error_response['thread'], channel={"name": SLACK_CHANNEL})  
                            else:
                                error_msg = 'Unknown xpath element'
                                error_response = self.slack_client.send_message(error_msg, channel={"name": SLACK_CHANNEL})
                                self.slack_client.send_message(post, thread=error_response['thread'], channel={"name": SLACK_CHANNEL})
                        else:
                            error_msg = 'Unknown xpath'
                            error_response = self.slack_client.send_message(error_msg, channel={"name": SLACK_CHANNEL})
                            self.slack_client.send_message(post, thread=error_response['thread'], channel={"name": SLACK_CHANNEL})
                    else:
                        error_msg = 'Unknown status code'
                        error_response = self.slack_client.send_message(error_msg, channel={"name": SLACK_CHANNEL})
                        self.slack_client.send_message(post, thread=error_response['thread'], channel={"name": SLACK_CHANNEL})
            except Exception as e:
                self.slack_client.send_message(str(e), channel={"name": SLACK_CHANNEL})

        
        print('Closing {} spider'.format(spider.name))
