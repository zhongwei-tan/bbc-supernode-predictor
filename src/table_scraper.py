from bs4 import BeautifulSoup
import requests
import pandas as pd
import os
from shared_mixin import SharedMixin


class TableScraper(SharedMixin):

    def __init__(self, url):
        self.max_csv_rows = 50000
        self.url = url
        super().__init__()

    @staticmethod
    def fetch_html(url):
        return requests.get(url)

    def parse(self, html):
        content = BeautifulSoup(html, "lxml")
        rows = content.find_all("tr")
        cells = [[c.text for c in r.find_all("td")] for r in rows]
        cell_df = pd.DataFrame(cells)
        cell_df = cell_df[cell_df[10].astype(int) > 0]

        node_data_dir = "node_data"
        if not os.path.exists(node_data_dir):
            os.mkdir(node_data_dir)

        for i, row in cell_df.iterrows():
            node_file_path = f'{node_data_dir}/{row[1]}.csv'
            if not os.path.exists(node_file_path):
                f = open(node_file_path, "w")
                f.close()
                csv_df = pd.DataFrame(columns=self.column_names)
            else:
                csv_df = pd.read_csv(node_file_path)

            new_data = pd.Series([self.datetime_now, row[3], row[10]],
                                 index=self.column_names)
            csv_df = csv_df.append(new_data, ignore_index=True)[-self.max_csv_rows:]
            csv_df.to_csv(node_file_path, index=False)

    def run(self):
        response = self.fetch_html(self.url)
        self.parse(response.text)


if __name__ == "__main__":
    scraper = TableScraper("http://bbcblock.cn/BlockView/GetVoteStatisticHandler.ashx")
    scraper.run()