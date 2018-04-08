import time
import datetime
import threading
import pymongo
import redis
from bson import json_util
import logging
from logging.handlers import RotatingFileHandler
import traceback
from multiprocessing.dummy import Pool as ThreadPool


"""
指定起始时间和结束时间，把 create_time 在此时间段内的国内 Mongo 数据写入 MongoDB 消息队列
"""


# 国内 MongoDB 连接信息
MONGODB_HOST = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'bj_sync_hk'
PASSWORD = '3e8beb9fb9a1'

# Redis 信息
REDIS_IP = '10.46.231.24'
REDIS_PORT = 6379
OPLOG_QUEUE = 'part_sync'
OSS_QUEUE = 'oss'

# 取MongoDB oplog数据的间隔，太小会导致生产数据太快而堆积数据，太大会导致数据取得太慢
INTERVAL = 0.002

# Redis 中队列的最大长度
MAX_OPLOG_SIZE = 1000000
MAX_OSS_SIZE = 1000000


class PartSyncProducer(threading.Thread):

    def __init__(self):
        """
        初始化
        """

        super(PartSyncProducer, self).__init__()

        # 需要同步的表
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
                       'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']

        start_time = '2018-3-1 0:0:0'
        end_time = '2018-4-1 0:0:0'

        self.start_time = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        self.end_time = datetime.datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

        # 记载 MongoDBListener 线程情况的 logger
        handle = RotatingFileHandler('./part_sync_producer.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handle)
        # self.logger.setLevel(logging.INFO)

        self.client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        admin = self.client['admin']
        admin.authenticate(USER, PASSWORD)

    def tablefetcher(self, table_name):

        db = table_name.split('.')[0]
        table = table_name.split('.')[1]

        cursor = self.client[db][table].find(
            {"$and": [
                {'create_time': {'$gte': self.start_time}},
                {'create_time': {'$lte': self.end_time}}
            ]})

        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

        for doc in cursor:

            # 检查 Redis 数据是否堆积太多
            oplog_siez = r.llen(OPLOG_QUEUE)
            oss_size = r.scard(OSS_QUEUE)
            if oplog_siez > MAX_OPLOG_SIZE or oss_size > MAX_OSS_SIZE:
                self.logger.warning('Redis 队列超过设置的长度限制，开始等候5分钟 ' +
                                    'OPLOG: ' + str(oplog_siez) + ' OSS: ' + str(oss_size))
                time.sleep(5 * 60)

            if table_name in ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.juchao_charts',
                              'cr_data.juchao_tables']:
                # cr_data.hb_charts,cr_data.hb_tables,cr_data.juchao_charts,cr_data.juchao_tables 表
                # 的 pngFile, fileUrl 字段有 OSS 链接

                if 'pngFile' in doc and doc['pngFile'] is not None:
                    pngFile_oss = doc['pngFile']
                    doc['pngFile'] = doc['pngFile'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                        .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    r.sadd(OSS_QUEUE, pngFile_oss)
                    self.logger.info(str(r.scard(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + pngFile_oss)

                if 'fileUrl' in doc and doc['fileUrl'] is not None:
                    fileUrl_oss = doc['fileUrl']
                    doc['fileUrl'] = doc['fileUrl'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                        .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    r.sadd(OSS_QUEUE, fileUrl_oss)
                    self.logger.info(str(r.scard(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + fileUrl_oss)

            elif table_name in ['cr_data.hb_text', 'cr_data.juchao_text']:
                # cr_data.hb_text, 'cr_data.juchao_text 表
                # 的 fileUrl, html_file, text_file, paragraph_file 字段有 OSS 链接
                if 'fileUrl' in doc and doc['fileUrl'] is not None:
                    fileUrl_oss = doc['fileUrl']
                    doc['fileUrl'] = doc['fileUrl'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                        .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    r.sadd(OSS_QUEUE, fileUrl_oss)
                    self.logger.info(str(r.scard(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + fileUrl_oss)

                if 'html_file' in doc and doc['html_file'] is not None:
                    html_file_oss = doc['html_file']
                    doc['html_file'] = doc['html_file'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                        .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    r.sadd(OSS_QUEUE, html_file_oss)
                    self.logger.info(str(r.scard(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + html_file_oss)

                if 'text_file' in doc and doc['text_file'] is not None:
                    text_file_oss = doc['text_file']
                    doc['text_file'] = doc['text_file'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                        .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    r.sadd(OSS_QUEUE, text_file_oss)
                    self.logger.info(str(r.scard(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + text_file_oss)

                if 'paragraph_file' in doc and doc['paragraph_file'] is not None:
                    paragraph_file_oss = doc['paragraph_file']
                    doc['paragraph_file'] = doc['paragraph_file']\
                        .replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                        .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    r.sadd(OSS_QUEUE, paragraph_file_oss)
                    self.logger.info(
                        str(r.scard(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + paragraph_file_oss)

            _id = doc['_id']
            del doc['_id']
            message = {'ns': table_name, '_id': _id, 'o':  {'$set': doc}}
            r.rpush(OPLOG_QUEUE, json_util.dumps(message, default=json_util.default))

            time.sleep(INTERVAL)

        self.logger.warning(table_name + '数据已经推送完')

    def run(self):
        pool = ThreadPool(len(self.tables))
        pool.map(self.tablefetcher, self.tables)


if __name__ == '__main__':
    PartSyncProducer().start()
