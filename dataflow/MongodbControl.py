# -*- coding: utf-8 -*-
from pymongo import MongoClient
import logging


class MongdbControl(object):

    def __init__(self, table_name, start_id=None):
        self.client = MongoClient('mongodb://search:ba3Re3ame+Wa@121.40.131.65:3718/cr_data', appname='chart_push')
        db = self.client['cr_data']
        self.collection = db[table_name]
        self.page_size = 100
        self.num = 0

        if start_id is None:
            self.cursor = self.collection.find().sort('_id')
        else:
            self.cursor = self.collection.find({'_id': {'$gt': start_id}}).sort('_id')

        logging.basicConfig(level=logging.WARNING,
                            filename='./log.txt',
                            filemode='w',
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    def __del__(self):
        self.client.close()

    def yield_data(self):

        if self.num % 10000 == 0:
            print('已经从mongodb读出{0}万条数据！！'.format(self.num))
        # logging.warning('数据ID：' + self.id + '，读取数据条数：' + str(self.num))
        record = self.cursor.next()
        while record:
            new_record = {}
            for i in record.keys():
                new_record['data:' + i] = str(record[i])
            self.num += 1
            record = self.cursor.next()
            yield new_record
import time
print(time.ctime() + 'sss')