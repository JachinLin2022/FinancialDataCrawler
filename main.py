from mongoManager import mongoManager
import logging


def init():
    logging.basicConfig(filename='log/log.log', format='%(asctime)s %(filename)s:%(lineno)d %(message)s',
                        level='DEBUG')
    logging.debug('-------------------------------------------------------------------------------------')
    logging.debug('FinancialDataCrawler start')
    global _mongoManager
    _mongoManager = mongoManager(27017)


if __name__ == '__main__':
    init()
    _mongoManager.insert_one('reviews', {
        'reviews': 'test',
        'rating': 3,
        'cuisine': 'aoligei'
    })
    res = _mongoManager.query_one('reviews', {'reviews': 'test'})
    print(res)
