import configparser
from pymongo import MongoClient
import pymongo
import time

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

    def get_data(self):

        if self.start_time is None:
            cursor = self.collection.find().sort('last_updated')
        else:
            cursor = self.collection.find({'last_updated': {'$gte': self.start_time}}).sort('last_updated')

        record = cursor.next()
        while record:
            new_record = {}
            # state 为 3,4,5 的是需要删除的
            # if record['state'] == 3 or record['state'] == 4 or record['state'] == 5:
            #     continue
            for i in record.keys():
                new_record[i] = str(record[i])
            record = cursor.next()
            yield new_record

    def yield_data(self):

        try:
            self.get_data()
        # 可能会出现连接错误，比如常见的pymongo.errors.AutoReconnect错误，这个时候会尝试重新连接5次
        except Exception:
            conf = configparser.ConfigParser()
            conf.read(CONFIG_FILE)

            user = conf.get("mongodb", "user")
            password = conf.get("mongodb", "password")
            host = conf.get("mongodb", "host")
            port = int(conf.get("mongodb", "port"))
            db_name = conf.get("mongodb", "db")
            table = conf.get("mongodb", "table")

            rounds = 6
            for i in range(rounds):
                try:
                    self.client = MongoClient(host, port)
                    self.db = self.client[db_name]
                    self.db.authenticate(user, password)
                    self.collection = self.db[table]

                    self.get_data()
                    break
                except Exception:
                    time.sleep(pow(2, i) * 60)
                    if i == rounds-1:
                        raise  # 如果最后一次还是失败，则抛出异常
