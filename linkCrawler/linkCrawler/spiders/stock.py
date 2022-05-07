import scrapy
from scrapy.linkextractors import LinkExtractor
import happybase
import json


class stockSpider(scrapy.Spider):
    name = 'stock'
    link_extractor = LinkExtractor()
    hbase = happybase.Connection(host='192.168.137.128')
    stock_table = hbase.table('stock')
    tmp_url = 'http://push2.eastmoney.com/api/qt/stock/trends2/get?secid={0}.{1}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58&ut=e1e6871893c6386c5ff6967026016627&iscr=0&cb=cb_1651494733237_97224201&isqhquote=&cb_1651494733237_97224201=cb_1651494733237_97224201'
    kline_url = 'http://71.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35109205286724146131_1651637878943&secid={0}.{1}&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&end=20500101&lmt=120&_=1651637878995'
    no=0
    map = {
        'hk': 'm:128+t:3,m:128+t:4,m:128+t:1,m:128+t:2',
        'us': 'm:105,m:106,m:107',
        'uk': 'm:155+t:1,m:155+t:2,m:155+t:3,m:156+t:1,m:156+t:2,m:156+t:5,m:156+t:6,m:156+t:7,m:156+t:8',
        'hs': 'm:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048'
    }
    def start_requests(self):
        url = 'http://80.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112408182158713421699_1651467716944&pn={0}&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs={1}&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1651467716945'
        for j in range(1, 250):
            yield scrapy.Request(url=url.format(j, self.map['hs']),
                                 callback=self.parse)

    def parse(self, response):
        rsp = str(response.body, 'utf-8')
        content = rsp
        start = content.find('{"rc"')
        end = content.find(');')
        lists = json.loads(content[start:end])['data']['diff']
        for i in lists:
            rowKey = i['f12']
            market = i['f13']
            d = {
                'info:board': json.dumps(i),
            }

            yield scrapy.Request(url=self.tmp_url.format(market, rowKey),
                                  callback=self.parse_trend, meta={'rowKey': rowKey, 'd': d})
            # yield scrapy.Request(url=self.kline_url.format(market, rowKey),
            #                      callback=self.parse_kline, meta={'rowKey': rowKey, 'd': d})

    def parse_trend(self, response):
        d = response.meta.get('d')
        rowKey = response.meta.get('rowKey')
        rsp = str(response.body, 'utf-8')
        content = rsp
        start = content.find('{"rc"')
        end = content.find(');')
        data = json.loads(content[start:end])['data']
        if data:
            trends = data['trends']
            d['info:trend'] = ';'.join(trends)
        else:
            d['info:trend'] = ''
        self.stock_table.put(rowKey, d)
        # print(d)

    def parse_kline(self, response):
        d = response.meta.get('d')
        rowKey = response.meta.get('rowKey')
        rsp = str(response.body, 'utf-8')
        content = rsp
        start = content.find('{"rc"')
        end = content.find(');')
        data = json.loads(content[start:end])['data']
        if data:
            klines = data['klines']
            d['info:kline'] = ';'.join(klines)
        else:
            d['info:kline'] = ''
        # print(d)
        self.stock_table.put(rowKey, d)
