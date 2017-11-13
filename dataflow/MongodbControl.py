import configparser
from pymongo import MongoClient

CONFIG_FILE = "path.conf"


class MongdbControl(object):

    def __init__(self, start_time=None):

        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)

        url = conf.get("mongodb", "url")
        appname = conf.get("mongodb", "appname")
        db = conf.get("mongodb", "db")
        table = conf.get("mongodb", "table")

        self.client = MongoClient(url, appname=appname)
        self.collection = self.client[db][table]
        self.page_size = 100

        if start_time is None:
            self.cursor = self.collection.find().sort('last_updated')
        else:
            self.cursor = self.collection.find({'last_updated': {'$gte': start_time}}).sort('last_updated')

    def __del__(self):
        self.client.close()

    def yield_data(self):
        record = self.cursor.next()
        while record:
            new_record = {}
            for i in record.keys():
                new_record[i] = str(record[i])
            record = self.cursor.next()
            yield new_record
