# -*- coding: utf-8 -*-
import HbaseControl
import MongodbControl
import time
import logging
import traceback
import MySQLControl
import datetime


def main_porcess(job_id, start_time=None, num=0):
    try:
        job_type = job_id.split(':')[0]
        origin = None
        destination = None
        if job_type == 'mongodb':
            origin = MongodbControl.MongdbControl(start_time)
            destination = HbaseControl.HbaseControl(table=b'hb_charts', column_families=[b'data'], put_num=num)
        elif job_type == 'mysql':
            origin = MySQLControl.MySQLControl(start_time)
            destination = HbaseControl.HbaseControl(table=b'hibor', column_families=[b'data'], put_num=num)

        records = []
        count = 0
        for i in origin.yield_data():
            if count >= 500:
                destination.puts(records, job_id)
                records = []
                count = 0
                if destination.put_num % 10000 == 0:
                    print(time.ctime() + '  Hbase 已经写入{0}万条数据'.format(destination.put_num / 10000))
                    logging.warning('Hbase 已经写入{0}万条数据'.format(destination.put_num / 10000))
            else:
                records.append(i)
                count += 1
        return True
    except Exception:
        logging.warning('==========重启程序==========')
        logging.warning(traceback.format_exc())
        print(time.ctime() + '==========重启程序==========')
        traceback.print_exc()

        f = open(job_id + '.txt')
        line = f.readlines()[0]
        log_dict = eval(line)
        my_job_id = log_dict['job_id']
        my_update = datetime.datetime.strptime(log_dict['update'], '%Y,%m,%d,%H,%M,%S')
        my_number = int(log_dict['number'])

        main_porcess(my_job_id, my_update, my_number)


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING,
                        filename='./process.log',
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    work_id = 'mysql:hibor'

    flag = False
    while not flag:
        flag = main_porcess(work_id)

