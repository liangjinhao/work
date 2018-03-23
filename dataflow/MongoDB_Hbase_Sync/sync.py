import time
import datetime
import threading
import pymongo
import bson
import json
import os
import traceback
import logging
from logging.handlers import RotatingFileHandler
from queue import Queue
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import hashlib
import time
from multiprocessing.dummy import Pool as ThreadPool


QUEUE = Queue(100000)

FULL_SYNC = True

# MongoDB 信息
MONGODB_HOST = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'bj_sync_hk'
PASSWORD = '3e8beb9fb9a1'

# Thrift 信息
THRIFT_IP = '10.27.71.108'
THRIFT_PORT = 9099
column_family = 'data'


class MongoDBHbaseSync(threading.Thread):

    def __init__(self):
        super(MongoDBHbaseSync, self).__init__()

        # 监听的表
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
                       'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']

    def push(self, table):
        """
        扫描 MongoDB 全表，并把数据写入Hbase 中
        :param table:
        :return:
        """

        handle = RotatingFileHandler('./full_sync.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        logger = logging.getLogger(table)
        logger.addHandler(handle)
        logger.setLevel(logging.INFO)
        logger.info('开始推送 ' + table + ' ！')

        db_name = table.split('.')[0]
        table_name = table.split('.')[1]

        client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        admin = client['admin']
        admin.authenticate(USER, PASSWORD)

        transport = TSocket.TSocket(THRIFT_IP, THRIFT_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        thrift_client = Hbase.Client(protocol)
        transport.open()

        count = 0
        cursor = client[db_name][table_name].find().sort('$natural', pymongo.ASCENDING)
        for record in cursor:
            count += 1
            mutations = []
            # row_key的值为 md5(_id)[0:10]:_id
            _id = str(record['_id'])
            row_key = bytes(hashlib.md5(bytes(_id, encoding="utf-8")).hexdigest()[0:10] + ':' + _id, encoding="utf-8")
            for item in record:
                if item == '_id':
                    continue
                key = bytes('data:' + item, encoding="utf8")
                var = bytes(str(record[item]), encoding="utf8")
                # hbase.client.keyvalue.maxsize 默认是10M，超出这个值则设置为None
                if len(var) < 10 * 1024 * 1024:
                    mutations.append(Hbase.Mutation(column=key, value=var))
                else:
                    mutations.append(Hbase.Mutation(column=key, value=bytes(str(None), encoding="utf8")))

            thrift_client.mutateRow(bytes(table_name, encoding="utf8"), row_key, mutations, {})

            if count % 100000 == 0:
                if 'create_time' in record:
                    logger.info(table + ' 已经读出 ' + str(count / 10000) + ' 万条数据'
                                + '    ' + str(record['create_time']))
                else:
                    logger.info(table + ' 已经读出 ' + str(count / 10000) + ' 万条数据')

        client.close()
        transport.close()

    def run(self):
        pool = ThreadPool(len(self.tables))
        pool.map(self.push, self.tables)


class MongodbFullSync(threading.Thread):

    def __init__(self):

        super(MongodbFullSync, self).__init__()

        # 监听的表
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
                       'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']

    def push_queue(self, table):

        handle = RotatingFileHandler('./mongodb_full_sync.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        logger = logging.getLogger(table)
        logger.addHandler(handle)
        logger.setLevel(logging.INFO)

        global QUEUE
        db_name = table.split('.')[0]
        table_name = table.split('.')[1]

        count = 0
        client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        admin = client['admin']
        admin.authenticate(USER, PASSWORD)

        cursor = client[db_name][table_name].find().sort('$natural', pymongo.ASCENDING)
        for record in cursor:
            count += 1
            if count % 100000 == 0:
                if 'create_time' in record:
                    logger.info(table + ' 已经读出 ' + str(count / 10000) + ' 万条数据'
                                + '    ' + str(record['create_time']))
                else:
                    logger.info(table + ' 已经读出 ' + str(count / 10000) + ' 万条数据')
            new_record = dict()
            new_record['op'] = 'i'
            new_record['table_name'] = table.split('.')[1]
            new_record['_id'] = str(record['_id'])
            new_record['columns'] = {}
            for i in record:
                if i == '_id':
                    continue
                new_record['columns'][i] = str(record[i])
            QUEUE.put(new_record)
        client.close()

    def run(self):
        pool = ThreadPool(len(self.tables))
        pool.map(self.push_queue, self.tables)


class MongodbIncrementalSync(threading.Thread):

    def __init__(self):

        super(MongodbIncrementalSync, self).__init__()

        # 记载 MongoDBListener 线程情况的 logger
        handle = RotatingFileHandler('./mongodb_incremental_sync.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger('MongodbIncrementalSync')
        self.logger.addHandler(handle)
        # self.logger.setLevel(logging.INFO)

        self.client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        admin = self.client['admin']
        admin.authenticate(USER, PASSWORD)

        # 监听的表
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
                       'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']

        oplog_begin_time = self.client.local.oplog.rs.find().sort('$natural', pymongo.ASCENDING).next()['ts']
        if os.path.exists('./listener_status'):
            status_time = json.loads(open('./listener_status').readline().strip())['sync_time']

            utc_offset_timedelta = datetime.datetime.utcnow() - datetime.datetime.now()
            utc_datetime = datetime.datetime.strptime(status_time, "%Y-%m-%d %H:%M:%S")
            result_local_datetime = utc_datetime - utc_offset_timedelta
            status_time_s = int(result_local_datetime.timestamp())

            if oplog_begin_time.time < status_time_s:
                self.start_ts = bson.timestamp.Timestamp(status_time_s, 1024)
            else:
                self.logger.warning('listener_status记载的时间 ' + status_time + ' 比oplog中最早的时间'
                                    + str(datetime.datetime.utcfromtimestamp(oplog_begin_time.time)) + ' 早')
                self.start_ts = oplog_begin_time
        else:
            self.start_ts = oplog_begin_time

        earliest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}).sort('$natural', pymongo.ASCENDING) \
            .next()
        latest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}).sort('$natural', pymongo.DESCENDING) \
            .next()

        # 记录监听状态
        self.status = {
            # 本次监听的 oplog 起始时间
            'start_time': str(datetime.datetime.utcfromtimestamp(self.start_ts.time)),
            # 本次监听最近一次同步操作的执行时间
            'time': str(datetime.datetime.now()).split('.')[0],
            # 本次监听最近一次同步的表和时间
            'sync_progress': '',
            # MongoDB 待同步端最早操作的表和时间
            'earliest_log': str(earliest_log['ns']) + str(datetime.datetime.utcfromtimestamp(earliest_log['ts'].time)),
            # MongoDB 待同步端最近操作的表和时间
            'latest_log': str(latest_log['ns']) + ': ' + str(datetime.datetime.utcfromtimestamp(latest_log['ts'].time)),
            # 本次监听监听到的操作总数
            'number': 0,
            # 每个 MongoSB 表更新日期, 累计更新次数等信息
            'table_info': {}
        }

        self.logger.warning('监听起始时间： ' + str(datetime.datetime.utcfromtimestamp(self.start_ts.time)))

    def run(self):
        global QUEUE

        while True:
            try:
                cursor = self.client.local.oplog.rs.find({'ts': {'$gte': self.start_ts}},
                                                         cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                                                         oplog_replay=True).sort('$natural', pymongo.ASCENDING)

                while cursor.alive:
                    write_interval = 5*60
                    write_ts = time.time()
                    for doc in cursor:
                        table_name = doc['ns']
                        self.status['number'] = self.status['number'] + 1
                        if table_name in self.tables:
                            op = doc['op']
                            new_doc = dict()
                            new_doc['op'] = 'd' if op == 'd' else 'i'
                            new_doc['table_name'] = doc['ns'].split('.')[1]
                            new_doc['_id'] = str(doc['o']['_id'] if '_id' in doc['o'] else str(doc['o2']['_id']))
                            new_doc['columns'] = {}

                            if 'o' in doc:
                                if op == 'd':
                                    pass
                                elif op == 'i':
                                    for i in doc['o']:
                                        new_doc['columns'][i] = str(doc['o'][i])
                                elif op == 'u':
                                    if '$set' in doc['o']:
                                        for i in doc['o']['$set']:
                                            new_doc['columns'][i] = str(doc['o']['$set'][i])
                                    else:
                                        for i in doc['o']:
                                            new_doc['columns'][i] = str(doc['o'][i])

                            QUEUE.put(new_doc)

                            self.logger.info(str(QUEUE.qsize()) + ' 读出 MongoDB ' + new_doc['_id'])

                            self.status['time'] = str(datetime.datetime.now()).split('.')[0]
                            self.status['sync_progress'] = str(doc['ns']) + ': ' \
                                + str(datetime.datetime.utcfromtimestamp(doc['ts'].time))

                            self.status['table_info'][table_name] = {
                                't_sync_progress': str(datetime.datetime.utcfromtimestamp(doc['ts'].time)),
                                't_number': 1 if table_name not in self.status['table_info']
                                else self.status['table_info'][table_name]['t_number'] + 1
                            }
                        if time.time() - write_ts > write_interval:
                            earliest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}})\
                                .sort('$natural', pymongo.ASCENDING).next()
                            latest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}})\
                                .sort('$natural', pymongo.DESCENDING).next()
                            self.status['ealiest_log'] = str(earliest_log['ns']) \
                                + str(datetime.datetime.utcfromtimestamp(earliest_log['ts'].time))
                            self.status['latest_log'] = str(latest_log['ns']) \
                                + str(datetime.datetime.utcfromtimestamp(latest_log['ts'].time))

                            write_ts = time.time()
                            open('./listener_status', 'w').write(json.dumps(self.status))

                    time.sleep(0.001)

            except Exception as e:
                earliest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}) \
                    .sort('$natural', pymongo.ASCENDING).next()
                latest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}) \
                    .sort('$natural', pymongo.DESCENDING).next()
                self.status['ealiest_log'] = str(earliest_log['ns']) \
                    + str(datetime.datetime.utcfromtimestamp(earliest_log['ts'].time))
                self.status['latest_log'] = str(latest_log['ns']) \
                    + str(datetime.datetime.utcfromtimestamp(latest_log['ts'].time))

                self.status['exception'] = traceback.format_exc()
                open('./listener_status', 'w').write(json.dumps(self.status))
                self.logger.exception(e)


class HbaseSync(threading.Thread):

    def __init__(self):
        super(HbaseSync, self).__init__()
        # 记载 HbaseSync 线程情况的 logger
        handle = RotatingFileHandler('./hbase_sync.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger('HbaseSync')
        self.logger.addHandler(handle)
        # self.logger.setLevel(logging.INFO)

    def write_hbase(self, data):
        """
        将数据写入 Hbase
        :param data: 对 Hbase 的一个操作，比如
        {
            'op': 'i',  # i 是插入, d 是删除
            'table_name': 'hb_charts',
            '_id': '121314125_img2',
            'columns': {
                'title' : 'This is a title'
            }
        }
        :return:
        """

        transport = TSocket.TSocket(THRIFT_IP, THRIFT_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()

        op = data['op']
        table_name = bytes(data['table_name'], "utf-8")
        row_key = bytes(hashlib.md5(bytes(data['_id'], "utf-8")).hexdigest()[0:10] + ':' + data['_id'], "utf-8")
        columns = data['columns'] if 'columns' in data else []

        if op == 'i':
            mutations = []
            for item in columns:
                if item == '_id':
                    continue
                key = bytes('data:' + item, encoding="utf8")
                var = bytes(str(columns[item]), encoding="utf8")
                # hbase.client.keyvalue.maxsize 默认是10M，超出这个值则设置为None
                if len(var) < 10 * 1024 * 1024:
                    mutations.append(Hbase.Mutation(column=key, value=var))
                else:
                    mutations.append(Hbase.Mutation(column=key, value=bytes(str(None), encoding="utf8")))
            client.mutateRow(table_name, row_key, mutations, {})
            self.logger.info(str(QUEUE.qsize()) + ' 插入到 Hbase ' + str(data))
        elif op == 'd':
            client.deleteAllRow(table_name, row_key, {})
            self.logger.info(str(QUEUE.qsize()) + ' 删除到 Hbase ' + str(data))
        transport.close()

    def run(self):
        global QUEUE
        record = None
        while True:
            if not record:
                try:
                    record = QUEUE.get(timeout=60 * 2)
                    QUEUE.task_done()
                except Exception:
                    self.logger.warning('数据更新到最新！待10分钟后继续.')
                    time.sleep(60 * 10)
            else:
                try:
                    self.write_hbase(record)
                    record = None
                except Exception:
                    self.logger.error(traceback.format_exc())
                    time.sleep(5)


if __name__ == '__main__':

    # MongoDBHbaseSync().start()

    if FULL_SYNC:
        MongodbFullSync().start()

    MongodbIncrementalSync().start()

    for i in range(5):
        HbaseSync().start()

    QUEUE.join()
