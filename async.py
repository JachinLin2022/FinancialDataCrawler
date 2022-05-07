import happybase
import json
import threading
import redis
import os
hbase = happybase.Connection(host='192.168.137.128')
stock_table = hbase.table('news')

