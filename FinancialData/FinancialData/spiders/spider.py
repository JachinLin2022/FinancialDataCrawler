import scrapy
from scrapy.linkextractors import LinkExtractor
link_extractor = LinkExtractor()
import redis
class NewsSpider(scrapy.Spider):
    name = 'news'
    link_extractor = LinkExtractor()
    redis = redis.Redis(host='localhost',port=6379)
    # start_urls = ['https://www.google.com/search?q=site%3Aeastmoney.com&tbm=nws']
    def start_requests(self):
        # for i in range(40):
        #     yield scrapy.Request(url='https://www.google.com/search?q=site%3Aeastmoney.com&tbm=nws&start={}'.format(first), callback=self.parse)
        #     first = first + 10
        base_url = 'https://www.bing.com/search?q=site%3Aeastmoney.com%2Fa%2F'
        for j in range(18000,19110):
            url = base_url + '&filters=ex1%3a\"ez5_{0}_{1}\"'.format(j,j+1)
            first = 0
            for i in range(20):
                yield scrapy.Request(url=url+'&first={}'.format(first), callback=self.parse)
                first = first + 10

    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('eastmoney.com/a/') >=0:
                print(link.url)
                self.redis.sadd('news:start_urls', link.url)
