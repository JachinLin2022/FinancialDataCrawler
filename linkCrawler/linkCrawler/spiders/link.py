import scrapy
from scrapy.linkextractors import LinkExtractor
link_extractor = LinkExtractor()
import redis
class linkSpider(scrapy.Spider):
    name = 'link'
    link_extractor = LinkExtractor()
    redis = redis.Redis(host='localhost',port=6379)
    # start_urls = ['https://www.google.com/search?q=site%3Aeastmoney.com&tbm=nws']
    def start_requests(self):
        urls = [
            'https://www.bing.com/search?q=site:finance.eastmoney.com/a/',
            'https://www.bing.com/search?q=site%3Astockstar.com+shtml'
        ]
        base_url = 'https://www.bing.com/search?q=site%3Astockstar.com+shtml'
        # query = ''
        for j in range(19000,19112):
            url = base_url + '&filters=ex1%3a\"ez5_{0}_{1}\"'.format(j,j+1)
            first = 0
            for i in range(20):
                yield scrapy.Request(url=url+'&first={}'.format(first), callback=self.parse)
                first = first + 10

    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('search') < 0:
                if self.redis.sismember('news:crawl_urls', link.url) == 0:
                    if self.redis.sadd('news:start_urls', link.url) == 1:
                        print(link.url)
