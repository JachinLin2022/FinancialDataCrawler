import scrapy
from scrapy.linkextractors import LinkExtractor
link_extractor = LinkExtractor()
import redis
class linkSpider(scrapy.Spider):
    name = 'link_copy'
    link_extractor = LinkExtractor()
    redis = redis.Redis(host='192.168.137.128',port=6379)
    # start_urls = ['https://www.google.com/search?q=site%3Aeastmoney.com&tbm=nws']
    def start_requests(self):
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36",
            "cookie":"_EDGE_V=1; SRCHD=AF=BDVEHC; MUID=13F3CBEBC4CD659D2594DA98C5CC64EE; MUIDB=13F3CBEBC4CD659D2594DA98C5CC64EE; SRCHUID=V=2&GUID=10FDE888A7EB42EBAA24DD297536C9E8&dmnchg=1; _UR=QS=0&TQS=0; _FP=hta=on; _HPVN=CS=eyJQbiI6eyJDbiI6MiwiU3QiOjAsIlFzIjowLCJQcm9kIjoiUCJ9LCJTYyI6eyJDbiI6MiwiU3QiOjAsIlFzIjowLCJQcm9kIjoiSCJ9LCJReiI6eyJDbiI6MiwiU3QiOjAsIlFzIjowLCJQcm9kIjoiVCJ9LCJBcCI6dHJ1ZSwiTXV0ZSI6dHJ1ZSwiTGFkIjoiMjAyMi0wNS0wMlQwMDowMDowMFoiLCJJb3RkIjowLCJHd2IiOjAsIkRmdCI6bnVsbCwiTXZzIjowLCJGbHQiOjAsIkltcCI6MTd9; _EDGE_S=SID=15153DC7FC2A6D1D13962C5DFD706C73&mkt=zh-cn&ui=zh-cn; _SS=SID=15153DC7FC2A6D1D13962C5DFD706C73; SUID=M; SNRHOP=I=&TS=; SRCHUSR=DOB=20220326&T=1651757482000&TPC=1651757483000; ZHCHATSTRONGATTRACT=TRUE; ipv6=hit=1651761084918&t=4; SRCHHPGUSR=SRCHLANG=zh-Hans&BZA=0&BRW=NOTP&BRH=M&CW=778&CH=714&SW=1536&SH=864&DPR=1.25&UTC=480&DM=1&HV=1651757483&WTS=63787354282&PV=14.0.0&NEWWND=1&NRSLT=50&LSL=0&AS=1&NNT=1&HAP=0&VSRO=1"
        }
        urls = [
            'site:finance.eastmoney.com/a/',
            # 'site%3Astockstar.com+shtml',
            # 'site%3Afinance.sina.com.cn+doc',
            # 'site:hexun.com html',
            # 'site:mp.cnfol.com'
        ]
        for url in urls:
            base_url = 'https://cn.bing.com/search?q=' + url
            # query = ''
            for j in range(19110,19111):
                url = base_url + '&filters=ex1%3a\"ez5_{0}_{1}\"'.format(j,j+1)
                first = 0
                for i in range(1):
                    yield scrapy.Request(url=url+'&first={}'.format(first), callback=self.parse, headers = headers)
                    first = first + 50

    def parse(self, response):
        for link in self.link_extractor.extract_links(response):
            if link.url.find('search') < 0 and link.url.find('bing') < 0 and link.url.find('html') >= 0:
                print(link.url)
                if self.redis.sismember('news:crawl_urls', link.url) == 0:
                    if self.redis.sadd('news:start_urls', link.url) == 1:
                        print(link.url)
