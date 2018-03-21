import time
import datetime
import threading
import pymongo
import bson
import redis
import json
import os
from bson import json_util
import logging
from logging.handlers import RotatingFileHandler
import traceback


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

# 取MongoDB oplog数据的间隔，太小会导致生产数据太快而堆积数据
INTERVAL = 0.002

# Redis 中队列的最大长度
MAX_OPLOG_SIZE = 500000
MAX_OSS_SIZE = 500000


class MongoDBListener(threading.Thread):

    def __init__(self):
        """
        初始化
        """

        super(MongoDBListener, self).__init__()

        # 记载 MongoDBListener 线程情况的 logger
        handle = RotatingFileHandler('./listener.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))
        handle.setLevel(logging.INFO)

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handle)

        self.client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        admin = self.client['admin']
        admin.authenticate(USER, PASSWORD)

        # 确定监听的开始时间，如果有日志世界则用日志时间接着监听
        oplog_time = self.client.local.oplog.rs.find().sort('$natural', pymongo.ASCENDING).limit(-1).next()['ts']
        if os.path.exists('./listener_status'):
            status_time = json.loads(open('./listener_status').readline().strip())['sync_time']
            status_time_s = int(datetime.datetime.strptime(status_time, '%Y-%m-%d %H:%M:%S').strftime('%s'))

            if oplog_time.time < status_time_s:
                self.start_ts = bson.timestamp.Timestamp(status_time, 1024)
            else:
                self.logger.warning('listener_status记载的时间 ' + status_time + '比oplog中最早的时间'
                                    + str(datetime.datetime.utcfromtimestamp(oplog_time.time)) + '早')
                self.start_ts = oplog_time
        else:
            self.start_ts = oplog_time

        # 监听的表
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
                       'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']

        last_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}).sort('$natural', pymongo.DESCENDING)\
            .limit(-1).next()
        # 记录监听状态
        self.status = {
            'start_time': str(datetime.datetime.utcfromtimestamp(self.start_ts.time)),  # 本次监听开始时间
            'time': str(datetime.datetime.now()).split('.')[0],  # 本次监听最近一次同步操作的执行时间
            'sync_time': '',  # 本次监听最近一次同步的操作的时间
            'oplog_new': str(last_log['ns']) + ': ' + str(datetime.datetime.utcfromtimestamp(last_log['ts'].time)),  # MongoDB 待同步端最新操作的表和时间
            'number': 0,  # 本次监听监听到的操作总数
            'table_info': {},  # 每个 MongoSB 表更新日期, 累计更新次数等信息
            'exception': ''  # 异常信息
        }

        self.logger.warning('监听起始时间： ' + str(datetime.datetime.utcfromtimestamp(self.start_ts.time)))

    def run(self):

        while True:
            try:
                cursor = self.client.local.oplog.rs.find({'ts': {'$gte': self.start_ts}},
                                                         cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                                                         oplog_replay=True)

                r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

                while cursor.alive:
                    write_interval = 5*60
                    write_ts = time.time()
                    for doc in cursor:
                        table_name = doc['ns']
                        if table_name in self.tables:

                            # 检查 Redis 数据是否堆积太多
                            oplog_siez = r.llen(OPLOG_QUEUE)
                            oss_size = r.llen(OSS_QUEUE)
                            if oplog_siez > MAX_OPLOG_SIZE or oss_size > MAX_OSS_SIZE:
                                self.logger.warning('Redis 队列超过设置的长度限制，开始等候5分钟 ' +
                                                    'OPLOG: ' + str(oplog_siez) + ' OSS: ' + str(oss_size))
                                time.sleep(5*60)
                                current_oplog_time = self.client.local.oplog.rs.find()\
                                    .sort('$natural', pymongo.ASCENDING).limit(-1).next()['ts'].time
                                current_op_time = doc['ts'].time
                                if current_oplog_time > current_op_time:
                                    self.logger.error('由于 Redis 队列堆积未被消费，长时间睡眠监听程序已造成数据丢失。现在 Oplog '
                                                      '的最早时间为 '
                                                      + str(datetime.datetime.utcfromtimestamp(current_oplog_time))
                                                      + ' ，而同步的最近一次操作时间为 '
                                                      + str(datetime.datetime.utcfromtimestamp(current_op_time)))

                            if table_name in ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.juchao_charts',
                                              'cr_data.juchao_tables']:
                                # cr_data.hb_charts,cr_data.hb_tables,cr_data.juchao_charts,cr_data.juchao_tables 表
                                # 的 pngFile, fileUrl 字段有 OSS 链接

                                if 'pngFile' in doc['o'] and doc['o']['pngFile'] is not None:
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
                                if 'fileUrl' in doc['o'] and doc['o']['fileUrl'] is not None:
                                    fileUrl_oss = doc['o']['fileUrl']
                                    doc['o']['fileUrl'] = doc['o']['fileUrl'].replace('oss-cn-hangzhou',
                                                                                      'oss-cn-hongkong')
                                    r.rpush(OSS_QUEUE, fileUrl_oss)
                                    self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + fileUrl_oss)

                                if 'html_file' in doc['o'] and doc['o']['html_file'] is not None:
                                    html_file_oss = doc['o']['html_file']
                                    doc['o']['html_file'] = doc['o']['html_file'].replace('oss-cn-hangzhou',
                                                                                          'oss-cn-hongkong')
                                    r.rpush(OSS_QUEUE, html_file_oss)
                                    self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + html_file_oss)

                                if 'text_file' in doc['o'] and doc['o']['text_file'] is not None:
                                    text_file_oss = doc['o']['text_file']
                                    doc['o']['text_file'] = doc['o']['text_file'].replace('oss-cn-hangzhou',
                                                                                          'oss-cn-hongkong')
                                    r.rpush(OSS_QUEUE, text_file_oss)
                                    self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + text_file_oss)

                                if 'paragraph_file' in doc['o'] and doc['o']['paragraph_file'] is not None:
                                    paragraph_file_oss = doc['o']['paragraph_file']
                                    doc['o']['paragraph_file'] = doc['o']['paragraph_file'].replace('oss-cn-hangzhou',
                                                                                                    'oss-cn-hongkong')
                                    r.rpush(OSS_QUEUE, paragraph_file_oss)
                                    self.logger.info(str(r.llen(OSS_QUEUE)) + '    Push to Redis OSS queue: ' + paragraph_file_oss)

                            r.rpush(OPLOG_QUEUE, json.dumps(doc, default=json_util.default))
                            _id = str(doc['o']['_id'] if '_id' in doc['o'] else doc['o2']['_id'])
                            self.logger.info(str(r.llen(OPLOG_QUEUE)) + '    Push to Redis oplog queue: '
                                             + _id + ' ' + doc['op'])

                            self.status['time'] = str(datetime.datetime.now()).split('.')[0]
                            self.status['sync_time'] = str(datetime.datetime.utcfromtimestamp(doc['ts'].time))
                            self.status['number'] = self.status['number'] + 1
                            self.status['table_info'][table_name] = {
                                't_sync_time': str(datetime.datetime.utcfromtimestamp(doc['ts'].time)),
                                'count': 1 if table_name not in self.status['table_info']
                                else self.status['table_info'][table_name]['count'] + 1
                            }

                        if time.time() - write_ts > write_interval:
                            last_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}})\
                                .sort('$natural', pymongo.DESCENDING).limit(-1).next()
                            self.status['oplog_new'] = str(last_log['ns']) + ': ' + \
                                str(datetime.datetime.utcfromtimestamp(last_log['ts'].time))

                            for table in self.tables:
                                self.status['table_info'][table]['t_oplog_new'] = \
                                    str(datetime.datetime.utcfromtimestamp(
                                        self.client.local.oplog.rs.find({'ns': table})
                                            .sort('$natural', pymongo.DESCENDING).limit(-1).next()))

                            write_ts = time.time()
                            open('./listener_status', 'w').write(json.dumps(self.status))

                        time.sleep(INTERVAL)

                    time.sleep(1)

            except Exception as e:
                last_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}) \
                    .sort('$natural', pymongo.DESCENDING).limit(-1).next()
                self.status['oplog_new'] = str(last_log['ns']) + ': ' + \
                    str(datetime.datetime.utcfromtimestamp(last_log['ts'].time))

                for table in self.tables:
                    self.status['table_info'][table]['t_oplog_new'] = \
                        str(datetime.datetime.utcfromtimestamp(
                            self.client.local.oplog.rs.find({'ns': table})
                                .sort('$natural', pymongo.DESCENDING).limit(-1).next()))

                self.status['exception'] = traceback.format_exc()
                open('./listener_status', 'w').write(json.dumps(self.status))
                self.logger.exception(e)
