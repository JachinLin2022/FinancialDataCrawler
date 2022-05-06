import requests
import json
import happybase

hbase = happybase.Connection(host='192.168.137.128')
stock_table = hbase.table('stock')
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36 QBCore/4.0.1326.400 QQBrowser/9.0.2524.400 Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2875.116 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x63010200)',
}


def get_kline(market, code):
    url = 'http://push2.eastmoney.com/api/qt/stock/trends2/get?secid={0}.{1}&fields1=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13&fields2=f51,f52,f53,f54,f55,f56,f57,f58&ut=e1e6871893c6386c5ff6967026016627&iscr=0&cb=cb_1651494733237_97224201&isqhquote=&cb_1651494733237_97224201=cb_1651494733237_97224201'
    r = requests.get(url.format(market, code), headers=header)
    content = r.text
    start = content.find('{"rc"')
    end = content.find(');')
    data = json.loads(content[start:end])['data']
    if data:
        trends = data['trends']
        return ';'.join(trends)
    else:
        return ''

# print(get_kline('02203'))
url = 'http://80.push2.eastmoney.com/api/qt/clist/get?cb=jQuery112408182158713421699_1651467716944&pn={0}&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs={1}&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=1651467716945'
for j in range(1, 2):
    r = requests.get(url.format(j, 'm:105,m:106,m:107'), headers=header)
    content = r.text
    start = content.find('{"rc"')
    end = content.find(');')
    lists = json.loads(content[start:end])['data']['diff']
    for i in lists:
        rowKey = i['f12']
        market = i['f13']
        d = {
            'info:board': json.dumps(i),
            'info:trend': get_kline(market, rowKey)
        }
        # for key in i:
        #     d['info:'+key] = i[key];
        stock_table.put(rowKey, d)
        print(i['f14'])
        # break
        # print(i)
