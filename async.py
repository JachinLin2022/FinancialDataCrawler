import happybase
import json
import threading
import redis
import os
from elasticsearch import Elasticsearch
hbase = happybase.Connection(host='192.168.137.128')
news_table = hbase.table('news')
es = Elasticsearch(hosts="http://192.168.137.128:9200")
i=0
for key, data in news_table.scan():
    title = str(key,'utf-8')
    es.index(index='financial_data',document={
        'title':title
    })