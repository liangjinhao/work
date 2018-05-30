import time
import datetime
import os
import json
import hashlib
import threading
from multiprocessing.dummy import Pool as ThreadPool
import pymongo
import bson
import traceback
import logging
from logging.handlers import RotatingFileHandler
from queue import Queue, Full, Empty
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import errno
import requests


# 是否全量更新
FULL_SYNC = True
# 是否进行增量更新
INCREMENTAL_SYNC = True

# MongoDB 信息
MONGODB_HOST = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'bj_sync_hk'
PASSWORD = '3e8beb9fb9a1'

# HBase Thrift 信息
THRIFT_IP = '10.27.71.108'
THRIFT_PORT = 9099
THRIFT_CONNECTION_NUM = 500  # Thrift 连接数量，数量越大，写入 HBase 越快，最好不要超过 1000

# 消费队列，MongoDB 读取的数据写入该队列，然后读取队列中的数据写入 HBase
QUEUE = Queue(100000)


"""
用于将 MongoDB 表的数据同步到 HBase 中，使用前请确认：
1. 对 MongoDB 集群的有效连接，并且该用户拥有对需要同步的表以及local表(local表用于增量同步操作)的读取权限
2. 对 HBase Thrift 的有效连接，并在操作前请确认 HBase 中已经建立了需要写入数据的表，此外，本脚本默认列族为 'data' (请检查 HBase 表的列族)
3. 数据从 MongoDB 中导入到 HBase 过程中， MongoDB 的 _id 与 HBase 的 rowkey 的映射关系由 HBaseSync.get_rowkey() 给出，可以自定义该函数
3. 使用前在 MongodbFullSync 和 MongodbIncrementalSync 的__init__函数里的 self.tables 定义好需要同步的表
4. 本脚本用于全量同步时是不可中断的，用于增量同步时是可中断的(中断信息保存在产生的 mongodb_incremental_status 文件里)，但注意增量同步间的时间间隔不要隔的太长，以免遗漏数据
"""


class MongodbFullSync(threading.Thread):
    """
    全量地读取 MongoDB 表中的数据，写入到消费队列中去
    """

    def __init__(self):

        super(MongodbFullSync, self).__init__()

        # 需要同步的 MongoDB collection
        # self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
        #                'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_text', 'cr_data.juchao_charts', 'cr_data.juchao_text']

        self.log_dir = './log'
        try:
            os.makedirs(self.log_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        # Queue 被塞满时，读取 MongoDB 的等候间隔
        self.wait_interval = 0.1

    def push_queue(self, table):
        """
        将 MongoDB 表里读取出的每个文档重新组织格式后写入消费队列里去
        :param table:
        :return:
        """
        start = time.time()
        handle = RotatingFileHandler(self.log_dir + '/' + table + '.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        logger = logging.getLogger(table)
        logger.addHandler(handle)
        logger.setLevel(logging.INFO)

        global QUEUE
        db_name = table.split('.')[0]
        table_name = table.split('.')[1]

        try:
            count = 0
            client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
            admin = client['admin']
            admin.authenticate(USER, PASSWORD)

            cursor = client[db_name][table_name].find().sort('$natural', pymongo.ASCENDING)
            for record in cursor:
                new_record = dict()
                new_record['op'] = 'i'
                new_record['table_name'] = table.split('.')[1]
                new_record['_id'] = str(record['_id'])
                new_record['columns'] = {}
                for i in record:
                    if i == '_id':
                        continue
                    new_record['columns'][i] = str(record[i])

                def safe_put(data, try_times=5):
                    if try_times > 0:
                        try:
                            try_times -= 1
                            QUEUE.put(data)
                        except Full:
                            logger.debug('消息队列数据已满，等候' + str(self.wait_interval) + '秒！')
                            time.sleep(self.wait_interval)
                            safe_put(try_times)
                        except Exception:
                            logger.error(traceback.format_exc())

                safe_put(new_record)
                count += 1

                if count % 10000 == 0:
                    if 'create_time' in record:
                        logger.info(table + ' 已经读出 ' + str(count / 10000) + ' 万条数据'
                                    + '    ' + str(record['create_time']))
                    else:
                        logger.info(table + ' 已经读出 ' + str(count / 10000) + ' 万条数据')
            end = time.time()
            logger.warning(table + ' 全量读出完成，累计读出 ' + str(count) + ' 条数据'
                           + '，共耗时 ' + str((end-start)/(60*60)) + ' 小时')
            client.close()
        except Exception:
            logger.error(traceback.format_exc())

    def run(self):
        pool = ThreadPool(len(self.tables))
        pool.map(self.push_queue, self.tables)


class MongodbIncrementalSync(threading.Thread):
    """
    增量地读取 MongoDB 表中的数据，写入到消费队列中去
    """

    def __init__(self):

        super(MongodbIncrementalSync, self).__init__()

        handle = RotatingFileHandler('./mongodb_incremental_sync.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger('MongodbIncrementalSync')
        self.logger.addHandler(handle)
        self.logger.setLevel(logging.INFO)

        self.client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        admin = self.client['admin']
        admin.authenticate(USER, PASSWORD)

        # 需要同步的 MongoDB 表
        # self.tables = ['cr_data.hb_charts', 'cr_data.hb_tables', 'cr_data.hb_text',
        #                'cr_data.juchao_charts', 'cr_data.juchao_tables', 'cr_data.juchao_text']
        self.tables = ['cr_data.hb_charts', 'cr_data.hb_text', 'cr_data.juchao_charts', 'cr_data.juchao_text']

        # 读取 MongoDB Oplog 的等候间隔
        self.wait_interval = 0.1

        oplog_begin_time = self.client.local.oplog.rs.find().sort('$natural', pymongo.ASCENDING).next()['ts']
        if os.path.exists('./mongodb_incremental_status'):
            status_time = json.loads(open('./mongodb_incremental_status').readline().strip())['sync_progress'].split(': ')[1]

            # status_time 存的不是 UTC 时间，先转换成 UTC 时间
            utc_offset_timedelta = datetime.datetime.utcnow() - datetime.datetime.now()
            utc_datetime = datetime.datetime.strptime(status_time, "%Y-%m-%d %H:%M:%S")
            result_local_datetime = utc_datetime - utc_offset_timedelta
            status_time_s = int(result_local_datetime.timestamp())

            if oplog_begin_time.time < status_time_s:
                self.start_ts = bson.timestamp.Timestamp(status_time_s, 1024)
            else:
                self.logger.warning('status 记载的时间 ' + status_time + ' 比 oplog 中最早的时间'
                                    + str(datetime.datetime.utcfromtimestamp(oplog_begin_time.time)) + ' 早，将从'
                                    + str(datetime.datetime.utcfromtimestamp(oplog_begin_time.time)) + '开始同步!')
                self.start_ts = oplog_begin_time
        else:
            self.start_ts = oplog_begin_time

        earliest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}).sort('$natural', pymongo.ASCENDING).next()
        latest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}).sort('$natural', pymongo.DESCENDING).next()

        # 记录监听状态
        self.status = {
            # 本次监听的 oplog 起始时间
            'start_time': str(datetime.datetime.utcfromtimestamp(self.start_ts.time)),
            # 本次监听最近一次同步操作的执行时间（本地时间）
            'time': str(datetime.datetime.now()).split('.')[0],
            # 本次监听最近一次同步的表和时间
            'sync_progress': '',
            # MongoDB 待同步端最早操作的表和时间
            'earliest_log': str(earliest_log['ns']) + ': ' + str(datetime.datetime.utcfromtimestamp(earliest_log['ts'].time)),
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
        count = 0
        write_status_interval = 5 * 60  # 保存推送情况 status 文件的间隔
        write_ts = time.time()
        while True:
            try:
                cursor = self.client.local.oplog.rs.find({'ts': {'$gte': self.start_ts}},
                                                         cursor_type=pymongo.CursorType.TAILABLE_AWAIT,
                                                         oplog_replay=True).sort('$natural', pymongo.ASCENDING)

                while cursor.alive:
                    for doc in cursor:
                        table_name = doc['ns']
                        self.status['number'] = self.status['number'] + 1
                        if table_name in self.tables:

                            count += 1
                            if count % 100000 == 0:
                                self.logger.info('已经同步' + str(count / 10000) + ' 万条操作记录')

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

                            def safe_put(data, try_times=5):
                                if try_times > 0:
                                    try:
                                        try_times -= 1
                                        QUEUE.put(data)
                                    except Full:
                                        self.logger.debug('消息队列数据已满，等候' + str(self.wait_interval) + '秒！')
                                        time.sleep(self.wait_interval)
                                        safe_put(try_times)
                                    except Exception:
                                        self.logger.error(traceback.format_exc())

                            safe_put(new_doc)

                            self.logger.debug(str(QUEUE.qsize()) + ' 读出 MongoDB ' + new_doc['_id'])

                            self.status['time'] = str(datetime.datetime.now()).split('.')[0]
                            self.status['sync_progress'] = str(doc['ns']) + ': ' \
                                + str(datetime.datetime.utcfromtimestamp(doc['ts'].time))

                            self.status['table_info'][table_name] = {
                                't_sync_progress': str(datetime.datetime.utcfromtimestamp(doc['ts'].time)),
                                't_number': 1 if table_name not in self.status['table_info']
                                else self.status['table_info'][table_name]['t_number'] + 1
                            }
                        if time.time() - write_ts > write_status_interval:
                            earliest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}})\
                                .sort('$natural', pymongo.ASCENDING).next()
                            latest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}})\
                                .sort('$natural', pymongo.DESCENDING).next()
                            self.status['ealiest_log'] = str(earliest_log['ns']) + ': '\
                                + str(datetime.datetime.utcfromtimestamp(earliest_log['ts'].time))
                            self.status['latest_log'] = str(latest_log['ns']) + ': '\
                                + str(datetime.datetime.utcfromtimestamp(latest_log['ts'].time))

                            write_ts = time.time()
                            open('./mongodb_incremental_status', 'w').write(json.dumps(self.status))

                            try:
                                # 给监测程序发送心跳post
                                post_url = 'http://10.168.117.133:2999/watch'
                                for key in self.status['table_info'].keys():
                                    sync_progress = self.status['table_info'][key]['t_sync_progress']
                                    requests.post(post_url,
                                                  data={
                                                      'name': 'MongodbHbaseSync_' + key.split('.')[1],
                                                      'time': sync_progress})
                            except:
                                pass

            except Exception as e:
                earliest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}) \
                    .sort('$natural', pymongo.ASCENDING).next()
                latest_log = self.client.local.oplog.rs.find({'ns': {'$in': self.tables}}) \
                    .sort('$natural', pymongo.DESCENDING).next()
                self.status['ealiest_log'] = str(earliest_log['ns']) + ': '\
                    + str(datetime.datetime.utcfromtimestamp(earliest_log['ts'].time))
                self.status['latest_log'] = str(latest_log['ns']) + ': '\
                    + str(datetime.datetime.utcfromtimestamp(latest_log['ts'].time))

                self.status['exception'] = traceback.format_exc()
                open('./mongodb_incremental_status', 'w').write(json.dumps(self.status))
                self.logger.exception(e)


class HBaseSync(threading.Thread):
    """
    消费队列中的数据，写入 HBase中，操作前请确认 HBase 中已经建立了该表。
    这里会将 MongoDB 表的所有字段写入 HBase 的同一个 columnn family 里，请事先在 HBase 中配置好表的列族设置
    """

    def __init__(self, columnn_family='data'):
        """
        初始化函数
        :param columnn_family: 写入到 HBase 的列族
        """

        super(HBaseSync, self).__init__()

        handle = RotatingFileHandler('./hbase_sync.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger('HBaseSync')
        self.logger.addHandler(handle)
        # self.logger.setLevel(logging.INFO)

        transport = TSocket.TSocket(THRIFT_IP, THRIFT_PORT)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        self.client = Hbase.Client(protocol)
        transport.open()

        self.cf = columnn_family

    @staticmethod
    def generate_rowkey(id):
        """
        将 MongoDB 源 id 转换成 HBase 的 rowkey， 为了提高 HBase 的性能，通常要避免 rowkey 是有序的。此函数会使然源 id的 md5 值’
        前10位再用冒号连接源 id，比如 '123456' -> 'e10adc3949:123456'
        :param id: MongoDB 源 id
        :return: 转换后的 HBase rowkey
        """

        return hashlib.md5(bytes(id, "utf-8")).hexdigest()[0:10] + ':' + id

    def write_hbase(self, data):
        """
        将数据写入 HBase， 注意，源ID会经过
        :param data: 对 HBase 的一个操作，比如
        {
            # 'i' 是插入, 'd' 是删除 (只能是 'i' 或 'd')
            'op': 'i',
            # 写入的 HBase 表
            'table_name': 'hb_charts',
            # 数据id
            '_id': '121314125_img2',
            # 写入的各个字段的值
            'columns': {
                'title' : 'This is a title'
            }
        }
        :return:
        """

        op = data['op']
        table_name = bytes(data['table_name'], "utf-8")
        #
        row_key = bytes(self.generate_rowkey(data['_id']), "utf-8")
        columns = data['columns'] if 'columns' in data else []

        if op == 'i':
            mutations = []
            for item in columns:
                if item == '_id':
                    continue
                key = bytes(self.cf + ':' + item, encoding="utf8")
                var = bytes(str(columns[item]), encoding="utf8")
                # hbase.client.keyvalue.maxsize 默认是10M，超出这个值则设置为None
                if len(var) < 10 * 1024 * 1024:
                    mutations.append(Hbase.Mutation(column=key, value=var))
                else:
                    mutations.append(Hbase.Mutation(column=key, value=bytes(str(None), encoding="utf8")))
                    self.logger.warning(self.getName() + ' ' + data['table_name'] + ' 的 _id为 ' + data['_id'] + ' 的数据的 '
                                        + str(item) + ' 字段的值大小超过了' + ' HBase 默认规定的键值10M限制，先已经置 None 替代该值')
            self.client.mutateRow(table_name, row_key, mutations, {})
            self.logger.debug(str(QUEUE.qsize()) + ' 插入到 HBase ' + str(data))
        elif op == 'd':
            self.client.deleteAllRow(table_name, row_key, {})
            self.logger.debug(str(QUEUE.qsize()) + ' 删除到 HBase ' + str(data))

    def run(self):
        global QUEUE
        record = None
        while True:
            if not record:
                try:
                    record = QUEUE.get(timeout=60 * 2)
                    QUEUE.task_done()
                except Empty:
                    self.logger.debug('消息队列中的数据被消费完，5s后继续！')
                    time.sleep(5)
                except Exception:
                    self.logger.error(traceback.format_exc())
            else:
                try:
                    self.write_hbase(record)
                    record = None
                except Exception:
                    self.logger.error(traceback.format_exc())
                    time.sleep(5)


if __name__ == '__main__':

    if FULL_SYNC:
        MongodbFullSync().start()

    if INCREMENTAL_SYNC:
        MongodbIncrementalSync().start()

    for i in range(THRIFT_CONNECTION_NUM):
        HBaseSync().start()

    QUEUE.join()
