import scrapy
from ..items import ArticleItem
from scrapy.linkextractors import LinkExtractor
import datetime


class FinancialarticleSpider(scrapy.Spider):
    name = 'financialArticle'
    link_extractor = LinkExtractor()
    i = 0

    filter_keyword = 'news.hexun.com'
    title_xpath = '//h1'
    post_time_xpath = '/html/body/div[5]/div/div[1]/span'
    author_xpath = '//div[@class="tip fl"]/a'
    content_xpath = '//div[@class="art_contextBox"]//p'

    def start_crawl_general(self):
        url = 'https://www.hexun.com/'
        yield scrapy.Request(url=url, callback=self.gen_link)

    def start_requests(self):
        return self.start_crawl_general()

    def start_crawl_sina(self):
        url = 'https://finance.sina.com.cn/'
        yield scrapy.Request(url=url, callback=self.gen_sina_link)

    def start_crawl_stockstar(self):
        begin = datetime.date(2022, 4, 1)
        end = datetime.date(2022, 5, 1)
        d = begin
        delta = datetime.timedelta(days=1)
        basic_url = 'https://finance.stockstar.com/list/1221_'
        while d <= end:
            url = basic_url + d.strftime("%Y%m%d") + '.shtml'
            yield scrapy.Request(url=url, callback=self.gen_stockstar_link)
            d += delta

    def start_crawl_eastmoney(self):
        url = 'https://www.eastmoney.com/'
        yield scrapy.Request(url=url, callback=self.gen_eastmoney_link)

    def gen_eastmoney_link(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('finance.eastmoney.com/a') is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_eastmoney_article)
            elif link.url.find('finance.eastmoney.com') is not -1:
                yield scrapy.Request(url=link.url, callback=self.gen_eastmoney_link)

    def parse_eastmoney_article(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('finance.eastmoney.com/a') is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_eastmoney_article)
            elif link.url.find('finance.eastmoney.com') is not -1:
                yield scrapy.Request(url=link.url, callback=self.gen_eastmoney_link)
        # if link.url.find('finance.eastmoney.com/a') is not -1:
        #     self.i = self.i + 1
        #     print(self.i, link.url)
        #     yield scrapy.Request(url=link.url, callback=self.parse_article)
        title = response.xpath('//*[@id="topbox"]/div[1]/text()').get() or ''
        post_time = response.xpath('//*[@id="topbox"]/div[3]/div[1]/div[1]/text()').get() or ''
        author = response.xpath('//*[@id="topbox"]/div[3]/div[1]/div[2]/text()').get() or ''
        author = author.strip().replace('\r\n', '')
        content = response.xpath('//*[@id="ContentBody"]/p/text()').getall() or ''
        content = ''.join(content)

        article = ArticleItem()
        article['title'] = title
        article['content'] = content
        article['author'] = author
        article['post_time'] = post_time
        yield article

    def gen_stockstar_link(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('finance.stockstar.com') is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_stockstar_article)

    def parse_stockstar_article(self, response):
        title = response.xpath('//*[@id="sta_dh"]/div/div[2]/div[1]/h1/text()').get() or ''
        post_time = response.xpath('//*[@id="pubtime_baidu"]/text()').get() or ''
        author = response.xpath('//*[@id="sta_dh"]/div/div[2]/div[2]/div[1]/div/span[1]/a/text()').get() or ''
        content = response.xpath('//*[@id="container-box"]/div[1]/div/p/text()').getall() or ''
        content = ''.join(content)
        # print(title, post_time, author)
        article = ArticleItem()
        article['title'] = title
        article['content'] = content
        article['author'] = author
        article['post_time'] = post_time
        yield article

    def gen_sina_link(self, response):
        # return scrapy.Request(url='https://finance.sina.com.cn/china/2022-04-13/doc-imcwipii4092180.shtml', callback=self.parse_sina_article)
        for link in self.link_extractor.extract_links(response):
            if link.url.find('shtml') is not -1 and link.url.find('finance.sina.com.cn') is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_sina_article)
            # elif link.url.find('finance.eastmoney.com') is not -1:
            #     yield scrapy.Request(url=link.url, callback=self.gen_sina_link)

    def parse_sina_article(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('shtml') is not -1 and link.url.find('finance.sina.com.cn') is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_sina_article)
            else:
                yield scrapy.Request(url=link.url, callback=self.gen_sina_link)
        title = response.xpath('/html/head/title/text()').get() or ''
        post_time = response.xpath('//*[@id="top_bar"]/div/div[2]/span[1]/text()').get() or ''
        author = response.xpath('//*[@id="top_bar"]/div/div[2]/span[2]/text()').get() or ''
        author = author.strip().replace('\r\n', '')
        content = response.xpath('//*[@id="artibody"]/p/text()').getall() or ''
        content = ''.join(content)

        article = ArticleItem()
        article['title'] = title
        article['content'] = content
        article['author'] = author
        article['post_time'] = post_time
        yield article

    def gen_link(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find(self.filter_keyword) is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_article)

    def parse_article(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find(self.filter_keyword) is not -1:
                yield scrapy.Request(url=link.url, callback=self.parse_article)
            else:
                yield scrapy.Request(url=link.url, callback=self.gen_link)
        title = response.xpath(self.title_xpath + '/text()').get() or ''
        post_time = response.xpath(self.post_time_xpath + '/text()').get() or ''
        author = response.xpath(self.author_xpath + '/text()').get() or ''
        author = author.replace('\n', '').strip()
        content = response.xpath(self.content_xpath + '/text()').getall() or ''
        content = ''.join(content)

        article = ArticleItem()
        article['title'] = title
        article['content'] = content
        article['author'] = author
        article['post_time'] = post_time
        yield article
