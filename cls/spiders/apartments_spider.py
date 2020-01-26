import scrapy
import re
import hashlib
from scrapy import Request
from datetime import datetime

class JobsSpider(scrapy.Spider):

    name = 'rooms'
    allowed_domains = ['craigslist.org']
    start_urls = ['https://sfbay.craigslist.org/search/sfc/roo']

    # start_urls = ["https://sfbay.craigslist.org/search/sfc/roo?s=2280"]

    def parse(self, response):
        rooms = response.xpath('//p[@class="result-info"]')

        for room in rooms:
            relative_url = room.xpath('a/@href').extract_first()
            absolute_url = response.urljoin(relative_url)
            title = room.xpath('a/text()').extract_first()
            address = \
                room.xpath('span[@class="result-meta"]/span[@class="result-hood"]/text()'
                           ).extract_first('')[2:-1]

            yield Request(absolute_url, callback=self.parse_page,
                          meta={'URL': absolute_url, 'Title': title,
                          'Address': address})

        relative_next_url = \
            response.xpath('//a[@class="button next"]/@href'
                           ).extract_first()
        absolute_next_url = \
            'https://sfbay.craigslist.org/search/sfc/roo' \
            + relative_next_url

        # absolute_next_url = "https://sfbay.craigslist.org/search/sfc/roo?s=2280"

        yield Request(absolute_next_url, callback=self.parse)

    def parse_page(self, response):
        url = response.meta.get('URL')
        title = response.meta.get('Title')
        address = response.meta.get('Address')

        description = ''.join(line for line in
                              response.xpath('//*[@id="postingbody"]/text()'
                              ).extract())
        sanitize_desc = re.sub('\s+', ' ', description)

        # compensation = response.xpath('//p[@class="attrgroup"]/span[1]/b/text()').extract_first()

        price = \
            response.xpath('//span[@class="postingtitletext"]/span[@class="price"]/text()'
                           ).extract_first()
        employment_type = \
            response.xpath('//p[@class="attrgroup"]/span[2]/b/text()'
                           ).extract_first()

        # posting_body = response.xpath('//section[@id="postingbody"]/b/text()').extract_first()

        posting_id = \
            response.xpath('//div[@class="postinginfos"]/p[@class="postinginfo"]/text()'
                           ).extract_first()[9:]
        posting_time = \
            response.xpath('//div[@class="postinginfos"]/p[2]/time/text()'
                           ).extract_first()
        updated_posting_time = \
            response.xpath('//div[@class="postinginfos"]/p[3]/time/text()'
                           ).extract_first() or 'n/a'
        uuid = hashlib.md5((posting_id + posting_time
                           + updated_posting_time
                           + sanitize_desc).encode('utf-8')).hexdigest()

        yield {
            'post_id': posting_id,
            'post_time': posting_time,
            'updated_post_time': updated_posting_time,
            'url': url,
            'title': title,
            'address': address,
            'price': price,
            'description': sanitize_desc,
            'post_status': 'new',
            'region': 'San Francisco',
            'state': 'CA',
            'uuid': uuid,
            'created_at': datetime.now()
            }



            