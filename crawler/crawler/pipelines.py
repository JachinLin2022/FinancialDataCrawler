# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import json
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

class ArticlePipeline:
    def __init__(self):
        self.ids_seen = set()

    def open_spider(self, spider):
        self.file = open('items.jl', 'a', encoding='utf-8')

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        if not item['title']:
            return
        adapter = ItemAdapter(item)
        if adapter['id'] in self.ids_seen:
            raise DropItem(f"Duplicate item found: {item!r}")
        line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False).encode('utf8')
        self.file.write(line.decode() + '\n')
        self.ids_seen.add(adapter['id'])
        return item