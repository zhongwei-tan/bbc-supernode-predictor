from celery.task import task
from table_scraper import TableScraper


@task
def collect_supernode_data():
    scraper = TableScraper("http://bbcblock.cn/BlockView/GetVoteStatisticHandler.ashx")
    scraper.run()

