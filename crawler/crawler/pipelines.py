# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
import pymongo


class ArticlePipeline:
    collection_name = 'scrapy_items'

    def __init__(self, mongo_uri, mongo_db):
        self.file = None
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'items')
        )

    def open_spider(self, spider):
        self.file = open('items.jl', 'w', encoding='utf-8')
        self.titles_seen = set()
        self.client = pymongo.MongoClient(port=self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.file.close()
        self.client.close()

    def process_item(self, item, spider):
        if not item['title'] or not item['content']:
            return DropItem("invalid item title found: %s" % item['title'])
        if item['title'] in self.titles_seen:
            return DropItem("Duplicate item title found: %s" % item['title'])
        else:
            # line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False).encode('utf8')
            # self.file.write(line.decode() + '\n')
            self.titles_seen.add(item['title'])
            try:
                self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
                return item
            except:
                return DropItem("Duplicate item title found: %s" % item['title'])
