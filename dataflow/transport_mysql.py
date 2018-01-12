import threading
import time
import datetime
import logging
from logging.handlers import RotatingFileHandler
from queue import Queue
import MySQLControl
import HbaseControl
import notify
import filelock

buffer_size = 50000
mysql_queue = Queue(buffer_size)


class MySQLProducerThread(threading.Thread):

    def __init__(self, job_id, start_time, start_id, log_lock):
        super(MySQLProducerThread, self).__init__()
        self.start_time = start_time
        self.start_id = start_id
        self.job_id = job_id
        self.log_lock = log_lock

    def run(self):
        global mysql_queue
        mysql = MySQLControl.MySQLControl(self.start_time, self.start_id)
        while True:
            try:
                for i in mysql.yield_data():
                    mysql_queue.put(i)
                    self.start_time = i['update_at']
                    self.start_id = i['id']
                time.sleep(60)
            except Exception as ex:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  ==========mysql生产线程重新连接==========')
                with self.log_lock:
                    logger.warning(self.job_id + '  ==========mysql生产线程重新连接==========')
                    logger.exception('mysql生产线程出现错误')
                time.sleep(60)
                mysql = MySQLControl.MySQLControl(self.start_time, self.start_id)


class MySQLConsumerThread(threading.Thread):

    def __init__(self, job_id, table_name, column_families, put_num, file_lock, log_lock):
        """
        初始化一个推送数据到 Hbase 的线程
        :param job_id: 任务id
        :param table_name: Hbase 表名，比如 b'hibor'
        :param column_families: Hbase 表的列族列表，比如 [b'data']
        :param put_num: Hbase 写入的历史数据条数， 仅供统计使用
        """
        super(MySQLConsumerThread, self).__init__()
        self.job_id = job_id
        self.table_name = table_name
        self.column_families = column_families
        self.put_num = int(put_num)
        self.put_sum = 0
        self.records = []
        self.records_size = 1000
        self.file_lock = file_lock
        self.log_lock = log_lock

    def run(self):
        global mysql_queue
        hbase = HbaseControl.HbaseControl(self.table_name, self.column_families, self.file_lock, self.put_num)
        while True:
            try:
                if len(self.records) < self.records_size:
                    for i in range(len(self.records), self.records_size):
                        try:
                            # 两分钟仍没有数据就抛出异常，并等待5分钟
                            try:
                                record = mysql_queue.get(timeout=60 * 2)
                                mysql_queue.task_done()
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

                if int(self.put_num / 10000) > self.put_sum:
                    self.put_sum = int(self.put_num/10000) + 1
                    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  Hbase 已经写入{0}万条数据'
                          .format(self.put_num / 10000))
                    with self.log_lock:
                        logger.warning(self.job_id + '  Hbase 已经写入{0}万条数据'.format(self.put_num / 10000))
            except Exception as ex:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  ==========mysql消费线程重新连接==========')
                with self.log_lock:
                    logger.warning(self.job_id + '  ==========mysql消费线程重新连接==========')
                    logger.exception('mysql消费线程出现错误')
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

    handle = RotatingFileHandler('./process_mysql.log', maxBytes=5 * 1024 * 1024, backupCount=1)
    handle.setLevel(logging.WARNING)
    log_formater = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    handle.setFormatter(log_formater)

    logger = logging.getLogger('Rotating log')
    logger.addHandler(handle)
    logger.setLevel(logging.WARNING)

    hibor_lock = filelock.FileLock('mongodb:hb_charts')
    hibor_log_lock = filelock.FileLock('process_mongodb.log')
    t = notify.NotifyThread('hibor', None, hibor_lock, None, hibor_log_lock)
    t.start()

    work_id = 'mysql:hibor'

    last = get_last_progress(work_id)
    _job_id = last['job_id']
    _update = last['update']
    _id = last['id']
    _number = last['number']

    p = MySQLProducerThread(_job_id, _update, _id, hibor_log_lock)
    c = MySQLConsumerThread(_job_id, b'hibor', [b'data'], _number, hibor_lock, hibor_log_lock)
    p.start()
    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + work_id + '  ==========启动mysql生产线程==========')
    time.sleep(2)
    c.start()
    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + work_id + '  ==========启动mysql消费线程==========')
    time.sleep(2)

    mysql_queue.join()
