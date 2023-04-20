import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from weibo.spiders.search import SearchSpider


# scrapy crawl search -a keyword=福建江夏学院 -a start_date=2023-01-01 -a end_date=2023-01-01

process = CrawlerProcess(get_project_settings())
process.crawl(SearchSpider,keyword='福州',start_date='2023-01-15',end_date='2023-01-20')
process.start()
