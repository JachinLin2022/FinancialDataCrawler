import scrapy
from ..items import ArticleItem
from scrapy.linkextractors import LinkExtractor


class FinancialarticleSpider(scrapy.Spider):
    name = 'financialArticle'
    start_urls = [
        'https://finance.eastmoney.com/',
    ]
    link_extractor = LinkExtractor()
    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('finance.eastmoney.com/a') is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_article)
                # print(link.url.find('finance.eastmoney.com'))
        # for info in response.xpath('//*[@id="cjdd_list"]/../../div/div/div/ul/li/a/@href'):
        #     url = info.get()
        #     # print(url)
        #     if url is not None:
        #         yield scrapy.Request(url=url, callback=self.parse_article)

    def parse_article(self, response):
        title = response.xpath('//*[@id="topbox"]/div[1]/text()').get() or ''
        post_time = response.xpath('//*[@id="topbox"]/div[3]/div[1]/div[1]/text()').get() or ''
        author = response.xpath('//*[@id="topbox"]/div[3]/div[1]/div[2]/text()').get() or ''
        author = author.strip()
        content = response.xpath('//*[@id="ContentBody"]/p/text()').getall() or ''
        content = ''.join(content)

        article = ArticleItem()
        article['title'] = title
        article['content'] = content
        article['author'] = author
        article['post_time'] = post_time
        yield article