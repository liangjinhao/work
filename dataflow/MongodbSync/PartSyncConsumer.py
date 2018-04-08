import pymongo
from pymongo.errors import DuplicateKeyError
import redis
import threading
import time
import json
from bson import json_util
from bson import *
import logging
from logging.handlers import RotatingFileHandler
import traceback
import re


"""
从国内的 bj-mogo-sync002 上的 Mongo 消息队列中取出 Mongo 操作历史记录，然后将这些操作更新到香港的 Mongo 集群
"""


# 香港 MongoDB 连接信息信息
MONGODB_HOST = 'dds-j6cd3f25db6afa741.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'hk_sync'
PASSWORD = '9c9df8aebf04'

# Redis 信息
REDIS_IP = '47.97.27.84'
REDIS_PORT = 6379
OPLOG_QUEUE = 'part_sync'


class PartSyncConsumer(threading.Thread):

    def __init__(self):
        super(PartSyncConsumer, self).__init__()

        # 记载 MongoDBPusher 线程情况的 logger
        handle = RotatingFileHandler('./part_sync_consumer.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(thread)d %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handle)
        # self.logger.setLevel(logging.INFO)

        self.client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)

    def run(self):
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

        while True:
            message = r.lpop(name=OPLOG_QUEUE)

            if message:

                    message = str(message, 'utf-8') if isinstance(message, bytes) else message
                    message = json_util.loads(message, object_hook=json_util.object_hook)

                    db = message['ns'].split(".")[0]
                    table_name = message['ns'].split(".")[-1]
                    database = self.client[db]
                    database.authenticate(USER, PASSWORD)
                    collection = database[table_name]

                    collection.update_one({'_id': message['_id']}, message['o'], upsert=True)
                    self.logger.info(str(r.llen(OPLOG_QUEUE)) + '    Update to HK MongoDB: ' + str(id))

            else:
                self.logger.info('Redis oplog 队列中无数据，等待10s再取')
                time.sleep(10)


if __name__ == '__main__':

    for i in range(50):
        PartSyncConsumer().start()
        print('MongoDB Part 推送线程 %s 开启', i)
