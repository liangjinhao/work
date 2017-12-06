import HbaseControl
import MongodbControl
import time
import logging
from logging.handlers import RotatingFileHandler
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
    job_type = job_id.split(':')[0]
    origin = None
    destination = None
    if job_type == 'mongodb':
        origin = MongodbControl.MongodbControl(start_time)
        destination = HbaseControl.HbaseControl(table=b'hb_charts', column_families=[b'data'], put_num=num)
    elif job_type == 'mysql':
        origin = MySQLControl.MySQLControl(start_time, start_id)
        destination = HbaseControl.HbaseControl(table=b'hibor', column_families=[b'data'], put_num=num)

    records = []
    count = 0
    label_time = time.time()
    for i in origin.yield_data():
        if count >= 1000:
            destination.puts(records, job_id)
            records = []
            count = 0
            if destination.put_num % 10000 == 0:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + job_id + '  Hbase 已经写入{0}万条数据'
                      .format(destination.put_num / 10000))
                logger.warning(job_id + '  Hbase 已经写入{0}万条数据'.format(destination.put_num / 10000))
        else:
            # 两分钟没有来数据，说明数据已经较少了，等一会儿再取
            if (time.time() - label_time) > 60*2:
                print(time.strftime('%Y-%m-%d %H:%M:%S') + '  ' + job_id + '  所有数据处理更新到最新！等待5分钟后继续.')
                logger.warning(job_id + '  所有数据处理完毕！！等待5分钟后继续.')
                time.sleep(60*5)
                label_time = time.time()
            records.append(i)
            count += 1
    return True


if __name__ == '__main__':
    handle = RotatingFileHandler('./process_mongodb.log', maxBytes=5 * 1024 * 1024, backupCount=1)
    handle.setLevel(logging.WARNING)
    log_formater = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    handle.setFormatter(log_formater)

    logger = logging.getLogger('Rotating log')
    logger.addHandler(handle)
    logger.setLevel(logging.WARNING)

    work_id = 'mysql:hibor'

    while True:

        try:
            last = get_last_progress(work_id)
            main_porcess(last['job_id'], start_time=last['update'], start_id=last['id'], num=last['number'])
        except Exception as e:
            logger.warning(work_id + '  ==========重启程序==========')
            logger.warning(str(e))
            print(work_id + '  ' + time.strftime('%Y-%m-%d %H:%M:%S') + '  ==========重启程序==========')
