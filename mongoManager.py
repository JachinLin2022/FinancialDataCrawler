from pymongo import MongoClient
import logging


class mongoManager:
    def __init__(self, port):
        self.client = MongoClient(port=port)
        self.db = self.client.financialData
        logging.debug('start mongoManager')

    def insert_one(self, collection, data):
        result = self.db[collection].insert_one(data)
        logging.debug('insert data to collection:{0}, inserted_id:{1}'.format(collection, result.inserted_id))

    def query_one(self, collection, sql):
        result = self.db[collection].find_one(sql)
        logging.debug('query data from collection:{0}, sql:{1}, res:{2}'.format(collection, sql, result))
        return result
