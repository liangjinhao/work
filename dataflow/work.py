# -*- coding: utf-8 -*-
import HbaseControl
import MongodbControl
import time
import logging
import traceback
import MySQLControl
import datetime


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


def main_porcess(job_id, start_time=None, start_id=None, num=0):
    try:
        job_type = job_id.split(':')[0]
        origin = None
        destination = None
        if job_type == 'mongodb':
            origin = MongodbControl.MongdbControl(start_time)
            destination = HbaseControl.HbaseControl(table=b'hb_charts', column_families=[b'data'], put_num=num)
        elif job_type == 'mysql':
            origin = MySQLControl.MySQLControl(start_time, start_id)
            destination = HbaseControl.HbaseControl(table=b'hibor', column_families=[b'data'], put_num=num)

        records = []
        count = 0
        for i in origin.yield_data():
            if count >= 1000:
                destination.puts(records, job_id)
                records = []
                count = 0
                if destination.put_num % 10000 == 0:
                    print(job_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  Hbase 已经写入{0}万条数据'
                          .format(destination.put_num / 10000))
                    logging.warning(job_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  Hbase 已经写入{0}万条数据'
                                    .format(destination.put_num / 10000))
            else:
                records.append(i)
                count += 1
        return True
    except Exception:
        logging.warning(job_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  ==========重启程序==========')
        # logging.warning(traceback.format_exc())
        print(job_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  ==========重启程序==========')
        # traceback.print_exc()

        last_info = get_last_progress(job_id)

        main_porcess(last_info['job_id'], start_time=last_info['update'],
                     start_id=last_info['id'], num=last_info['number'])


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING,
                        filename='./process.log',
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    work_id = 'mysql:hibor'

    while True:
        last = get_last_progress(work_id)
        flag = main_porcess(last['job_id'], start_time=last['update'], start_id=last['id'], num=last['number'])

        if flag:
            print(work_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  所有数据处理完毕！等待1小时后重新开始:')
            print(get_last_progress(work_id))
            logging.warning(work_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  所有数据处理完毕！等待1小时后重新开始:')
            logging.warning(work_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  '+str(get_last_progress(work_id)))
            time.sleep(60 * 60)
