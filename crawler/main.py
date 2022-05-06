from twisted.internet import reactor
from crawler.spiders.financialArticle import FinancialarticleSpider
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner

class tset1(FinancialarticleSpider):
    a = 1


settings = get_project_settings()
configure_logging()
runner = CrawlerRunner(settings)
runner.crawl(tset1)
d = runner.join()
d.addBoth(lambda _: reactor.stop())
reactor.run()
