# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem
from itemadapter import ItemAdapter
from elasticsearch import Elasticsearch
import happybase

class NewscrawlerPipeline:
    def open_spider(self, spider):
        self.es = Elasticsearch(hosts="http://localhost:9200")
        self.hbase = happybase.Connection(host='hadoop1')
        self.news_table = self.hbase.table('news')


    def process_item(self, item, spider):
        if not item['title'] or not item['content']:
            return DropItem("invalid item title found: %s" % item['title'])
        # if item['title'] in self.titles_seen:
        #     return DropItem("Duplicate item title found: %s" % item['title'])
        else:
            # line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False).encode('utf8')
            # self.file.write(line.decode() + '\n')
            # self.titles_seen.add(item['title'])
            try:
                # self.db[self.collection_name].insert_one(ItemAdapter(item).asdict())
                d = {
                    'article:content': item['content'],
                    'article:url': item['url'],
                    'article:title': item['title']
                }
                self.news_table.put(item['title'],data=d)
                # item['content'] = item['content'][0:100] + '...'
                # self.es.index(index='financial_data', document=ItemAdapter(item).asdict())

                return item
            except:
                return DropItem("Duplicate item title found: %s" % item['title'])
