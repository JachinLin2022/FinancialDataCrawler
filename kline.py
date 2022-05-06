import requests
import json
import happybase

hbase = happybase.Connection(host='192.168.137.128')
stock_table = hbase.table('stock')
header = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36 QBCore/4.0.1326.400 QQBrowser/9.0.2524.400 Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2875.116 Safari/537.36 NetType/WIFI MicroMessenger/7.0.20.1781(0x6700143B) WindowsWechat(0x63010200)',
}
url = 'http://71.push2his.eastmoney.com/api/qt/stock/kline/get?cb=jQuery35109205286724146131_1651637878943&secid=1.{}&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&end=20500101&lmt=120&_=1651637878995'
r = requests.get(url.format('000001'), headers=header)
content = r.text
start = content.find('{"rc"')
end = content.find(');')
data = json.loads(content[start:end])['data']
klines = data['klines']
# d = {
#     'info:trend': ';'.join(trends)
# }
print(klines)
# stock_table.put('000001', d)
