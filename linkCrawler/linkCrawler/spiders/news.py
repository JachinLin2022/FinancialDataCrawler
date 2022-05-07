import logging

import scrapy
from readability import Document
from lxml import etree
import redis
from elasticsearch import Elasticsearch
import happybase
class newsSpider(scrapy.Spider):
    name = 'news'
    redis_client = redis.Redis(host='192.168.137.128', port=6379)
    hbase = happybase.Connection(host='192.168.137.128')
    es = Elasticsearch(hosts="http://localhost:9200")
    news_table = hbase.table('news')
    size = 1024
    # logging.Logger.setLevel('INFO')
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
        article = {
            'article:content':content,
            'article:title':title,
            'article:url':response.url
        }
        print(title)
        self.news_table.put(title, data=article)
        self.es.index(index='financial_data', document={
            'title': title,
            'content': content,
            'url': response.url[0:50],
        })

                
        
        
