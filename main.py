from flask import Flask
from flask import request
from flask_cors import CORS
import happybase
import json
import threading
import redis

hbase = happybase.Connection(host='192.168.137.128')
stock_table = hbase.table('stock')
app = Flask(__name__)
app.debug = True
CORS(app, supports_credentials=True)
redis = redis.Redis(host='192.168.137.128', port=6379)

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
    crawl_size = redis.scard('news:crawl_urls')
    start_size = redis.scard('news:start_urls')
    return {
        'crawl_size': crawl_size,
        'start_size': start_size
    }

# @app.route('/get_start_len', methods=['POST'])
# def get_start_len():
#     size = redis.scard('news:start_urls')
#     return {'size': size}
