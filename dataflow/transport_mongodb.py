import threading
import time
import datetime
import logging
from logging.handlers import RotatingFileHandler
from queue import Queue
import MongodbControl
import HbaseControl
import notify
import filelock

buffer_size = 50000
mongodb_queue = Queue(buffer_size)


class MongodbProducerThread(threading.Thread):

    def __init__(self, job_id, start_time, log_lock):
        super(MongodbProducerThread, self).__init__()
        self.start_time = start_time
        self.job_id = job_id
        self.log_lock = log_lock

    def run(self):
        global mongodb_queue
        mongodb = MongodbControl.MongodbControl(self.start_time)
        while True:
            try:
                for i in mongodb.yield_data():
                    mongodb_queue.put(i)
                    self.start_time = i['last_updated']
                time.sleep(60)
            except Exception as ex:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  ==========mongodb生产线程重新连接==========')
                with self.log_lock:
                    logger.warning(self.job_id + '  ==========mongodb生产线程重新连接==========')
                    logger.warning(str(ex))
                time.sleep(60)
                mongodb = MongodbControl.MongodbControl(self.start_time)


class MongodbConsumerThread(threading.Thread):

    def __init__(self, job_id, table_name, column_families, put_num, file_lock, log_lock):
        """
        初始化一个推送数据到 Hbase 的线程
        :param job_id: 任务id
        :param table_name: Hbase 表名，比如 b'hibor'
        :param column_families: Hbase 表的列族列表，比如 [b'data']
        :param put_num: Hbase 写入的历史数据条数， 仅供统计使用
        """
        super(MongodbConsumerThread, self).__init__()
        self.job_id = job_id
        self.table_name = table_name
        self.column_families = column_families
        self.put_num = put_num
        self.records = []
        self.records_size = 1000
        self.file_lock = file_lock
        self.log_lock = log_lock

    def run(self):
        global mongodb_queue
        hbase = HbaseControl.HbaseControl(self.table_name, self.column_families, self.file_lock, self.put_num)
        while True:
            try:
                if len(self.records) < self.records_size:
                    for i in range(len(self.records), self.records_size):
                        try:
                            # 两分钟仍没有数据就抛出异常，并等待5分钟
                            try:
                                record = mongodb_queue.get(timeout=60*2)
                                mongodb_queue.task_done()
                                self.records.append(record)
                            except Exception:
                                if self.records:
                                    hbase.puts(self.records, self.job_id)
                                time.sleep(60 * 5)
                        except Exception:
                            print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  数据更新到最新！待10分钟后继续.')
                            with self.log_lock:
                                logger.warning(self.job_id + '  数据更新到最新！待10分钟后继续.')
                            time.sleep(60 * 10)

                hbase.puts(self.records, self.job_id)
                self.put_num += len(self.records)
                self.records = []

                if self.put_num % 10000 == 0:
                    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  ' + '  Hbase 已经写入{0}万条数据'
                          .format(self.put_num / 10000))
                    with self.log_lock:
                        logger.warning(self.job_id + '  Hbase 已经写入{0}万条数据'.format(self.put_num / 10000))
            except Exception as ex:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  ==========mongodb消费线程重新连接==========')
                with self.log_lock:
                    logger.warning(self.job_id + '  ==========mongodb消费线程重新连接==========')
                    logger.warning(str(ex))
                time.sleep(60)
                hbase = HbaseControl.HbaseControl(self.table_name, self.column_families, self.file_lock, self.put_num)


def get_last_progress(job_id):
    f = open(job_id + '.txt')
    line = f.readlines()[0]
    log_dict = eval(line)
    f.close()
    my_job_id = log_dict['job_id']
    my_update = None
    if job_id.split(':')[0] == 'mongodb':
        my_update = datetime.datetime.strptime(log_dict['update'], '%Y-%m-%d %H:%M:%S.%f')
    elif job_id.split(':')[0] == 'mysql':
        my_update = datetime.datetime.strptime(log_dict['update'], '%Y-%m-%d %H:%M:%S')
    my_id = log_dict['id']
    my_number = int(log_dict['number'])
    return {'job_id': my_job_id, 'update': my_update, 'id': my_id, 'number': my_number}


if __name__ == '__main__':

    handle = RotatingFileHandler('./process_mongodb.log', maxBytes=5*1024*1024, backupCount=1)
    handle.setLevel(logging.WARNING)
    log_formater = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    handle.setFormatter(log_formater)

    logger = logging.getLogger('Rotating log')
    logger.addHandler(handle)
    logger.setLevel(logging.WARNING)

    hb_charts_lock = filelock.FileLock('mongodb:hb_charts')
    hb_charts_log_lock = filelock.FileLock('process_mongodb.log')
    hibor_lock = filelock.FileLock('mongodb:hb_charts')
    hibor_log_lock = filelock.FileLock('process_mongodb.log')
    t = notify.NotifyThread(hb_charts_lock, hibor_lock, hb_charts_log_lock, hibor_log_lock)
    t.start()

    work_id = 'mongodb:hb_charts'

    last = get_last_progress(work_id)
    _job_id = last['job_id']
    _update = last['update']
    _id = last['id']
    _number = last['number']

    p = MongodbProducerThread(_job_id, _update, hb_charts_log_lock)
    c = MongodbConsumerThread(_job_id, b'hb_charts', [b'data'], _number, hb_charts_lock, hb_charts_log_lock)
    p.start()
    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + work_id + '  ==========启动mongodb生产线程==========')
    time.sleep(2)
    c.start()
    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + work_id + '  ==========启动mongodb消费线程==========')
    time.sleep(2)

    mongodb_queue.join()
