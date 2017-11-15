from threading import Thread
import time
import datetime
import logging
import random
from queue import Queue
import MySQLControl
import HbaseControl

buffer_size = 5000
mysql_queue = Queue(buffer_size)


class MySQLProducerThread(Thread):

    def __init__(self, job_id, start_time, start_id):
        super(MySQLProducerThread, self).__init__()
        self.start_time = start_time
        self.start_id = start_id
        self.job_id = job_id

    def run(self):
        global mysql_queue

        while True:
            try:
                mysql = MySQLControl.MySQLControl(self.start_time, self.start_id)
                for i in mysql.yield_data():
                    mysql_queue.put(i)
                    self.start_time = i['update_at']
                    self.start_id = i['id']
                    # time.sleep(random.random())
            except Exception as ex:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  ==========mysql生产线程重新连接==========')
                logging.warning(self.job_id + '  ==========mysql生产线程重新连接==========')
                logging.warning(str(ex))


class MySQLConsumerThread(Thread):

    def __init__(self, job_id, table_name, column_families, put_num):
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
        self.put_num = put_num
        self.records = []
        self.records_size = 1000

    def run(self):
        global mysql_queue
        while True:
            try:
                hbase = HbaseControl.HbaseControl(self.table_name, self.column_families, self.put_num)
                action_time = time.time()
                if len(self.records) < self.records_size:
                    for i in range(len(self.records), self.records_size):

                        # 2分钟没有来数据，说明数据已经较少了，等5分钟再取
                        if (time.time() - action_time) > 60 * 2:
                            print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  数据更新到最新！待5分钟后继续.')
                            logging.warning(self.job_id + '  数据更新到最新！待5分钟后继续.')
                            time.sleep(60 * 5)
                            action_time = time.time()

                        record = mysql_queue.get()
                        mysql_queue.task_done()
                        self.records.append(record)
                hbase.puts(self.records, self.job_id)
                self.put_num += len(self.records)
                self.records = []
                # time.sleep(random.random())

                if self.put_num % 10000 == 0:
                    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  Hbase 已经写入{0}万条数据'
                          .format(self.put_num / 10000))
                    logging.warning(self.job_id + '  Hbase 已经写入{0}万条数据'.format(self.put_num / 10000))
            except Exception as ex:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + self.job_id + '  ==========mysql消费线程重新连接==========')
                logging.warning(self.job_id + '  ==========mysql消费线程重新连接==========')
                logging.warning(str(ex))


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
    logging.basicConfig(level=logging.WARNING,
                        filename='./process.log',
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    work_id = 'mysql:hibor'

    last = get_last_progress(work_id)
    _job_id = last['job_id']
    _update = last['update']
    _id = last['id']
    _number = last['number']

    p = MySQLProducerThread(_job_id, _update, _id)
    c = MySQLConsumerThread(_job_id, b'hibor', [b'data'], _number)
    p.start()
    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + work_id + '  ==========启动mysql生产线程==========')
    time.sleep(2)
    c.start()
    print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + work_id + '  ==========启动mysql消费线程==========')
    time.sleep(2)

    mysql_queue.join()
