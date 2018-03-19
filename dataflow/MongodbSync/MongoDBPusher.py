import pymongo
from pymongo.errors import DuplicateKeyError
import redis
import threading
import time
import json
from bson import json_util
import logging
from logging.handlers import RotatingFileHandler


# MongoDB 信息
MONGODB_HOST = 'dds-j6cd3f25db6afa741.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'hk_sync'
PASSWORD = '9c9df8aebf04'

# Redis 信息
REDIS_IP = '47.97.27.84'
REDIS_PORT = 6379
OPLOG_QUEUE = 'oplog'


class MongoDBPusher(threading.Thread):

    def __init__(self):
        super(MongoDBPusher, self).__init__()

        # 记载 MongoDBPusher 线程情况的 logger
        handle = RotatingFileHandler('./mongodb_pusher.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))
        handle.setLevel(logging.INFO)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handle)

        self.client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)

    def run(self):
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

        while True:
            oplog_data = r.lpop(name=OPLOG_QUEUE)

            if oplog_data:
                oplog_data = json.loads(oplog_data, object_hook=json_util.object_hook)

                try:
                    action_type = oplog_data['op']
                    db = oplog_data['ns'].split(".")[0]
                    table_name = oplog_data['ns'].split(".")[-1]
                    database = self.client[db]
                    database.authenticate(USER, PASSWORD)
                    collection = database[table_name]

                    _id = oplog_data['o']['_id'] if '_id' in oplog_data['o'] else oplog_data['o2']['_id']

                    if action_type == 'i':
                        try:
                            collection.insert_one(oplog_data['o'])
                            self.logger.info(str(r.llen(OPLOG_QUEUE)) + '    Insert to HK MongoDB: ' + _id)
                        except DuplicateKeyError:
                            oplog_data['op'] = 'd'
                            collection.delete_one({'_id': oplog_data['o']['_id']})
                            oplog_data['op'] = 'i'
                            collection.insert_one(oplog_data['o'])
                            self.logger.info(str(r.llen(OPLOG_QUEUE)) + '    Insert to HK MongoDB: ' + _id)
                    elif action_type == 'u':
                        collection.update_one(oplog_data['o2'], oplog_data['o'])
                        self.logger.info(str(r.llen(OPLOG_QUEUE)) + '    Update to HK MongoDB: ' + _id)
                    elif action_type == 'd':
                        collection.delete_one(oplog_data['o'])
                        self.logger.info(str(r.llen(OPLOG_QUEUE)) + '    Delete to HK MongoDB: ' + _id)
                except Exception as e:
                    r.rpush(OPLOG_QUEUE, oplog_data)
                    self.logger.error(str(r.llen(OPLOG_QUEUE)) + '    操作 HK MongoDB 失败，重新加到 Redis 队列末尾。 '
                                      'oplog 为: ' + str(oplog_data) + '错误为: ' + str(e))
            else:
                self.logger.info('Redis oplog 队列中无数据，等待10s再取')
                time.sleep(10)
