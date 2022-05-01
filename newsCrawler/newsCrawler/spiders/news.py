from scrapy_redis.spiders import RedisSpider
import scrapy
from readability import Document
from lxml import etree
from ..items import ArticleItem
import redis
from threading import Timer
# import scrapy_redis.scheduler.Scheduler
class newsSpider(scrapy.Spider):
    name = 'news'
    redis_client = redis.Redis(host='localhost', port=6379)
    size = 1024
    def get_url_from_redis(self):
        urls = self.redis_client.spop('news:start_urls', self.size)
        for url in urls:
            if self.redis_client.sadd('news:crawl_urls', str(url, 'utf-8')) == 1:
                yield scrapy.Request(url=str(url, 'utf-8'), callback=self.parse)

    def start_requests(self):
        return self.get_url_from_redis()
       

    def parse(self, response):
        doc = Document(response.text)
        summary = doc.summary()
        title = doc.title()
        content_element = etree.HTML(summary)
        content = ''
        for p in content_element.xpath('//p//text()'):
            content = content + p
        article = ArticleItem()
        article['content'] = content
        article['title'] = title
        article['url'] = response.url
        yield article
                
        
        
