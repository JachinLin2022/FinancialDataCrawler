from twisted.internet import reactor
from linkCrawler.spiders.link import linkSpider
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner
import time
class tset1(linkSpider):
    a = 1


settings = get_project_settings()
configure_logging()

# runner = CrawlerRunner(settings)
# runner.crawl(linkSpider)
# d = runner.join()
# d.addBoth(lambda _: reactor.run())
# reactor.run()
def crawl(runner):
    d = runner.crawl(linkSpider)
    d = runner.join()
    d.addBoth(lambda _: reactor.run())
    return d

runner = CrawlerRunner(settings)
crawl(runner)
reactor.run()