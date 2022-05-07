from flask import Flask
from flask import request
from flask_cors import CORS
import happybase
import json
import threading
import redis
import os
from elasticsearch import Elasticsearch

hbase = happybase.Connection(host='192.168.137.128')
stock_table = hbase.table('stock')
news_table = hbase.table('news')
app = Flask(__name__)
app.debug = True
CORS(app, supports_credentials=True)
redis_clients = []
redis_clients.append(redis.Redis(host='192.168.137.128', port=6379))
redis_clients.append(redis.Redis(host='192.168.137.129', port=6379))
es = Elasticsearch(hosts="http://192.168.137.128:9200")

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route('/get_stock_board', methods=['POST'])
def get_stock_board():
    start = request.form.get('start')
    limit = request.form.get('limit')
    res = {
        'data': []
    }
    for key, data in stock_table.scan(columns=['info:board'],limit=int(limit),row_start=start):
        data = str(data[b'info:board'], 'utf-8')
        res['data'].append(json.loads(data))
    return res

@app.route('/get_stock_trend', methods=['POST'])
def get_stock_trend():
    stockNo = request.form.get('stockNo')
    row = stock_table.row(stockNo, columns=['info:trend'])
    data = str(row[b'info:trend'], 'utf-8').split(';')
    return {'data':data}

@app.route('/get_stock_kline', methods=['POST'])
def get_stock_kline():
    stockNo = request.form.get('stockNo')
    row = stock_table.row(stockNo, columns=['info:kline'])
    data = str(row[b'info:kline'], 'utf-8').split(';')
    return {'data':data}

@app.route('/get_redis_info', methods=['POST'])
def get_crawl_len():
    crawl_size = redis_clients[0].scard('news:crawl_urls')
    start_size = redis_clients[0].scard('news:start_urls')
    return {
        'crawl_size': crawl_size,
        'start_size': start_size
    }

@app.route('/run_spider', methods=['POST'])
def run_spider():
    spider = request.form.get('spider')
    # print(spider.split('.')[0])
    # settings = get_project_settings()
    # configure_logging()
    # runner = CrawlerRunner(settings)
    # d = runner.crawl(linkSpider)
    os.system('cd linkCrawler && scrapy crawl {}'.format(spider.split('.')[0]))
    return 'ok'

@app.route('/get_spider', methods=['POST'])
def get_spider():
    spider = request.form.get('spider')
    print(spider)
    with open('linkCrawler/linkCrawler/spiders/{}'.format(spider), encoding='utf-8') as f:
        return f.read()


@app.route('/save_spider', methods=['POST'])
def save_spider():
    spider = request.form.get('spider')
    content = request.form.get('content')
    if not content:
        return 'content is null'
    with open('linkCrawler/linkCrawler/spiders/{}'.format(spider), encoding='utf-8', mode='w') as f:
        # content = str(content, 'utf-8')
        # print(content)
        f.write(content)
        return 'ok'

@app.route('/search_article', methods=['POST'])
def search_article():
    keyword = request.form.get('keyword')
    body = {
        'query':{
            'match':{
                'title':keyword
            }
        }
    }
    search = es.search(index='financial_data', body=body)
    format_res = []
    for hit in search['hits']['hits']:
        format_res.append({
            'title': hit['_source']['title'],
            'content': hit['_source']['content'],
            'url': hit['_source']['url'],
        })
    return {
        'data':format_res
    }


@app.route('/get_article', methods=['POST'])
def get_article():
    title = request.form.get('title')
    row = news_table.row(title)
    format_data = {
        'content':str(row[b'article:content'],'utf-8'),
        'title':str(row[b'article:title'],'utf-8'),
        'url':str(row[b'article:url'],'utf-8'),
    }
    # print(format_data)
    return format_data