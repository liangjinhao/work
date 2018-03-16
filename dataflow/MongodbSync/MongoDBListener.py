import time
import datetime
import threading
import pymongo
import bson
import redis
import json
from bson import json_util
import logging
from logging.handlers import RotatingFileHandler


# MongoDB 信息
MONGODB_HOST = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'bj_sync_hk'
PASSWORD = '3e8beb9fb9a1'

# Redis 信息
REDIS_IP = '10.46.231.24'  # 10.46.231.24 47.97.27.84
REDIS_PORT = 6379
OPLOG_QUEUE = 'oplog'
OSS_QUEUE = 'oss'


class MongoDBListener(threading.Thread):

    def __init__(self, start_time=None, tables=None):
        """
        初始化
        :param start_time: 监听的起始时间，格式为 '%Y-%m-%d %H:%M:%S'，比如 '2018-3-14 3:26:0'
        :param tables: 需要监听的表名列表，格式为 ['db_name.table_name', ...]，比如 ['cr_data.hb_charts']
        """

        super(MongoDBListener, self).__init__()

        # 记载 MongoDBListener 线程情况的 logger
        handle = RotatingFileHandler('./MongoDBListener.log', maxBytes=5 * 1024 * 1024, backupCount=5)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handle)

        self.client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)

        self.client = pymongo.MongoClient(host=MONGODB_HOST, port=MONGODB_PORT)
        admin = self.client['admin']
        admin.authenticate('bj_sync_hk', '3e8beb9fb9a1')

        try:
            epoch = int(datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S').strftime('%s'))
        except Exception as e:
            if start_time is not None:
                self.logger.warning(e)
            epoch = 0

        if epoch:
            self.start_ts = bson.timestamp.Timestamp(epoch, 1024)
        else:
            self.start_ts = self.client.local.oplog.rs.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()['ts']

        self.logger.info('监听起始时间： ' + str(datetime.datetime.utcfromtimestamp(self.start_ts.time)))

        self.tables = tables
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
                       'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']

    def run(self):
        while True:
            cursor = self.client.local.oplog.rs.find({'ts': {'$gt': self.start_ts}},
                                                     cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                                                     oplog_replay=True)

            r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

            while cursor.alive:
                for doc in cursor:
                    table_name = doc['ns']
                    if table_name in self.tables:

                        if table_name in ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.juchao_charts',
                                          'cr_data.juchao_tables']:
                            # cr_data.hb_charts,cr_data.hb_tables,cr_data.juchao_charts,cr_data.juchao_tables 表
                            # 的 pngFile, fileUrl 字段有 OSS 链接

                            if 'pngFile' in doc['o']:
                                pngFile_oss = doc['o']['pngFile']
                                doc['o']['pngFile'] = doc['o']['pngFile'].replace('oss-cn-hangzhou',
                                                                                  'oss-cn-hongkong')
                                r.rpush(OSS_QUEUE, pngFile_oss)
                                self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + pngFile_oss)

                            if 'fileUrl' in doc['o']:
                                fileUrl_oss = doc['o']['fileUrl']
                                doc['o']['fileUrl'] = doc['o']['fileUrl'].replace('oss-cn-hangzhou',
                                                                                  'oss-cn-hongkong')
                                r.rpush(OSS_QUEUE, fileUrl_oss)
                                self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + fileUrl_oss)

                        elif table_name in ['cr_data.hb_text', 'cr_data.juchao_text']:
                            # cr_data.hb_text, 'cr_data.juchao_text 表
                            # 的 fileUrl, html_file, text_file, paragraph_file 字段有 OSS 链接
                            if 'fileUrl' in doc['o']:
                                fileUrl_oss = doc['o']['fileUrl']
                                doc['o']['fileUrl'] = doc['o']['fileUrl'].replace('oss-cn-hangzhou',
                                                                                  'oss-cn-hongkong')
                                r.rpush(OSS_QUEUE, fileUrl_oss)
                                self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + fileUrl_oss)

                            if 'html_file' in doc['o']:
                                html_file_oss = doc['o']['html_file']
                                doc['o']['html_file'] = doc['o']['html_file'].replace('oss-cn-hangzhou',
                                                                                      'oss-cn-hongkong')
                                r.rpush(OSS_QUEUE, html_file_oss)
                                self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + html_file_oss)

                            if 'text_file' in doc['o']:
                                text_file_oss = doc['o']['text_file']
                                doc['o']['text_file'] = doc['o']['text_file'].replace('oss-cn-hangzhou',
                                                                                      'oss-cn-hongkong')
                                r.rpush(OSS_QUEUE, text_file_oss)
                                self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + text_file_oss)

                            if 'paragraph_file' in doc['o']:
                                paragraph_file_oss = doc['o']['paragraph_file']
                                doc['o']['paragraph_file'] = doc['o']['paragraph_file'].replace('oss-cn-hangzhou',
                                                                                                'oss-cn-hongkong')
                                r.rpush(OSS_QUEUE, paragraph_file_oss)
                                self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + paragraph_file_oss)

                        r.rpush(OPLOG_QUEUE, json.dumps(doc, default=json_util.default))
                        _id = doc['o']['_id'] if '_id' in doc['o'] else doc['o2']['_id']
                        self.logger.info(str(r.llen(OPLOG_QUEUE)) + '    Push to Redis oplog queue: ' + _id)

                time.sleep(1)
