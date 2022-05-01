from twisted.internet import reactor
from newsCrawler.spiders.news import newsSpider
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner
import time
import redis
class tset1(newsSpider):
    a = 1


settings = get_project_settings()
configure_logging()
redis_client = redis.Redis(host='localhost', port=6379)
# runner = CrawlerRunner(settings)
# runner.crawl(linkSpider)
# d = runner.join()
# d.addBoth(lambda _: reactor.run())
# reactor.run()
def crawl(runner):
    count = redis_client.scard('news:start_urls')
    while count <= 1024:
        count = redis_client.scard('news:start_urls')
        print(count)
        time.sleep(5)
    d = runner.crawl(newsSpider)
    d = runner.join()
    d.addBoth(lambda _: crawl(runner))
    return d
    
    

runner = CrawlerRunner(settings)
crawl(runner)
reactor.run()