import scrapy
from ..items import ArticleItem
from scrapy.linkextractors import LinkExtractor
import datetime
import json
from urllib.parse import unquote

class FinancialarticleSpider(scrapy.Spider):
    name = 'financialArticle'
    link_extractor = LinkExtractor()
    i = 0
    config = json.load(open("./config.json", "r"))
    site_map = config['site_map']
    roll_site_map = config['roll_site_map']
    url_map = json.load(open("./url.json", "r"))

    def start(self):
        for url_info in self.site_map:
            if url_info['valid'] == 1:
                yield scrapy.Request(url=url_info['url'], callback=self.gen_link, meta={'pattern': url_info['pattern']})

    def start_roll(self):
        for url_info in self.roll_site_map:
            if url_info['valid'] == 1:
                for i in range(5,100):
                    url = url_info['roll_url'].format(i)
                    yield scrapy.Request(url=url, callback=self.gen_roll_link, meta={'pattern': url_info['pattern']})

    def start_requests(self):
        # return self.start_roll()
        return self.start()
    def gen_link(self, response):
        pattern = response.meta.get('pattern')
        for link in self.link_extractor.extract_links(response):
            valid = 1
            # index = link.url.find(pattern['filter_keyword'][0])
            # if index > 30 or index < 0:
            #     valid=0

            for key in pattern['filter_keyword']:
                if link.url.find(key) == -1:
                    valid = 0
                    break
            if valid:
                # self.url_map[link.url] = 1
                yield scrapy.Request(url=link.url, callback=self.parse_article, meta={'pattern': pattern})

    def parse_article(self, response):
        pattern = response.meta.get('pattern')
        for link in self.link_extractor.extract_links(response):
            # index = link.url.find(pattern['filter_keyword'][0])
            # if index > 30 or index < 0:
            #     continue
            valid = 1
            for key in pattern['filter_keyword']:
                if link.url.find(key) == -1:
                    valid = 0
                    break
            if valid:
                yield scrapy.Request(url=link.url, callback=self.parse_article, meta={'pattern': pattern})
            else:
                yield scrapy.Request(url=link.url, callback=self.gen_link, meta={'pattern': pattern})
        title = response.xpath(pattern['title_xpath'] + '/text()').get() or ''
        post_time = response.xpath(pattern['post_time_xpath'] + '/text()').get() or ''
        author = response.xpath(pattern['author_xpath'] + '/text()').get() or ''
        author = author.replace('\n', '').strip()
        content = response.xpath(pattern['content_xpath'] + '/text()').getall() or ''
        content = ''.join(content)

        article = ArticleItem()
        article['title'] = title
        article['content'] = content
        article['author'] = author
        article['post_time'] = post_time
        yield article

    def gen_roll_link(self, response):
        pattern = response.meta.get('pattern')
        for link in self.link_extractor.extract_links(response):
            valid = 1
            for key in pattern['filter_keyword']:
                if link.url.find(key) == -1:
                    valid = 0
                    break
            if valid:
                # self.url_map[link.url] = 1
                if link.url.find('https://cj.sina.cn/article/norm_detail?url=') >=0:
                    link.url = link.url.replace('https://cj.sina.cn/article/norm_detail?url=', '')
                    link.url = link.url.replace('&finpagefr=p_112', '')
                    link.url = unquote(link.url)
                    # print(unquote(link.url))
                yield scrapy.Request(url=link.url, callback=self.parse_roll_article, meta={'pattern': pattern})

    def parse_roll_article(self, response):
        pattern = response.meta.get('pattern')
        title = response.xpath(pattern['title_xpath'] + '/text()').get() or ''
        post_time = response.xpath(pattern['post_time_xpath'] + '/text()').get() or ''
        author = response.xpath(pattern['author_xpath'] + '/text()').get() or ''
        author = author.replace('\n', '').strip()
        content = response.xpath(pattern['content_xpath'] + '/text()').getall() or ''
        content = ''.join(content)

        article = ArticleItem()
        article['title'] = title
        article['content'] = content
        article['author'] = author
        article['post_time'] = post_time
        yield article

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

    def gen_stockstar_link(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('finance.stockstar.com') != -1:
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
