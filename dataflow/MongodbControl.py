import configparser
from pymongo import MongoClient

CONFIG_FILE = "path.conf"


class MongodbControl(object):

    def __init__(self, start_time=None):

        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)

        user = conf.get("mongodb", "user")
        password = conf.get("mongodb", "password")
        host = conf.get("mongodb", "host")
        port = int(conf.get("mongodb", "port"))
        db_name = conf.get("mongodb", "db")
        table = conf.get("mongodb", "table")

        self.client = MongoClient(host, port)
        self.db = self.client[db_name]
        self.db.authenticate(user, password)
        self.collection = self.db[table]
        self.start_time = start_time

    def __del__(self):
        self.client.close()

    def yield_data(self):

        if self.start_time is None:
            cursor = self.collection.find().sort('last_updated')
        else:
            cursor = self.collection.find({'last_updated': {'$gte': self.start_time}}).sort('last_updated')

        record = cursor.next()
        while record:
            new_record = {}
            for i in record.keys():
                new_record[i] = str(record[i])
            record = cursor.next()
            yield new_record
