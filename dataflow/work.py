# -*- coding: utf-8 -*-
import HbaseControl
import MongodbControl
import time
import logging
import traceback
import MySQLControl
import re
import datetime
import random


def main_porcess(job_type, start_time=None, num=0):
    origin = None
    if job_type == 'mongodb':
        origin = MongodbControl.MongdbControl(start_time)
    elif job_type == 'mysql':
        origin = MySQLControl.MySQLControl(start_time)
    destination = HbaseControl.HbaseControl(table=b'mongo', column_families=[b'data'], put_num=num)
    records = []
    count = 0
    for i in origin.yield_data():
        if count >= 500:
            destination.puts(records, key=origin.key)
            records = []
            count = 0
            if destination.put_num % 10000 == 0:
                print(time.ctime() + '  Hbase 已经写入{0}万条数据'.format(destination.put_num / 10000))
                logging.warning('Hbase 已经写入{0}万条数据'.format(destination.put_num / 10000))
        else:
            records.append(i)
            count += 1
    return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING,
                        filename='./process.log',
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    work_type = 'mysql'

    flag = True
    while flag:
        try:
            flag = main_porcess(work_type)
        except Exception as e:
            logging.warning('==========重启程序==========')
            logging.warning(traceback.format_exc())
            print(time.ctime() + '==========重启程序==========')
            traceback.print_exc()
            lines = open('./serialization.log').readlines()
            _start_time = None
            if work_type == 'mongodb':
                time_str = re.search("(?<='update_at': datetime.datetime\()[, \d]+(?=\),)", lines[0])[0]
                _start_time = datetime.datetime.strptime(time_str, '%Y, %m, %d, %H, %M, %S')
            elif work_type == 'mysql':
                time_str = re.search("(?<='update_at': datetime.datetime\()[, \d]+(?=\),)", lines[0])[0]
                _start_time = datetime.datetime.strptime(time_str, '%Y, %m, %d, %H, %M, %S')
            _num = int(lines[1])
            flag = main_porcess(work_type, _start_time, _num)


