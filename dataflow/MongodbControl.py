# -*- coding: utf-8 -*-
from pymongo import MongoClient


class MongdbControl(object):

    def __init__(self, table_name, start_id=None):
        # mongodb://search:ba3Re3ame+Wa@121.40.131.65:3718/cr_data
        # mongodb://search:ba3Re3ame%2bWa@dds-bp1d09d4b278ceb42.mongodb.rds.aliyuncs.com:3717,dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com:3717/cr_data
        self.client = MongoClient('mongodb://search:ba3Re3ame%2bWa@dds-bp1d09d4b278ceb42.mongodb.rds.aliyuncs.com:3717,dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com:3717/cr_data', appname='chart_push')
        db = self.client['cr_data']
        self.collection = db[table_name]
        self.page_size = 100
        self.num = 0

        if start_id is None:
            self.cursor = self.collection.find().sort('_id')
        else:
            self.cursor = self.collection.find({'_id': {'$gt': start_id}}).sort('_id')

    def __del__(self):
        self.client.close()

    def yield_data(self):
        record = self.cursor.next()
        while record:
            new_record = {}
            for i in record.keys():
                new_record['data:' + i] = str(record[i])
            self.num += 1
            record = self.cursor.next()
            yield new_record
