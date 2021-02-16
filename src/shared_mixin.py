from datetime import datetime


class SharedMixin:

    def __init__(self):
        self.column_names = ["datetime", "vote_amount", "block_amount"]
        self.datetime_format = "%d/%m/%Y %H:%M"

    @staticmethod
    def get_supernode_from_path(path: str):
        return path[path.rindex("/")+1:-4]

    @property
    def datetime_now(self):
        return datetime.now().strftime(self.datetime_format)