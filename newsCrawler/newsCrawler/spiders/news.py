from scrapy_redis.spiders import RedisSpider
from readability import Document
from lxml import etree
from ..items import ArticleItem
class newsSpider(RedisSpider):
    name = 'news'
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
                
        
        
