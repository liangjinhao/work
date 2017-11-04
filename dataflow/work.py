# -*- coding: utf-8 -*-
import HbaseControl
import MongodbControl
import time
import logging
import traceback


def main_porcess(id, num):
    hc = HbaseControl.HbaseControl(table=b'hb_charts', column_families=[b'data'], put_num=num)
    mc = MongodbControl.MongdbControl('hb_charts', start_id=id)
    records = []
    count = 0
    for i in mc.yield_data():
        if count >= 500:
            hc.puts(records)
            records = []
            count = 0
            if hc.put_num % 10000 == 0:
                print(time.ctime() + '  Hbase 已经写入{0}万条数据'.format(hc.put_num / 10000))
                logging.warning('Hbase 已经写入{0}万条数据'.format(hc.put_num / 10000))
        else:
            records.append(i)
            count += 1
    return False


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING,
                        filename='./process.log',
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    flag = True
    while flag:
        try:
            flag = main_porcess(id=None, num=0)
        except Exception as e:
            logging.warning('==========-重启程序-==========')
            logging.warning(traceback.format_exc())
            logging.warning('==========+重启程序+==========')
            print(time.ctime() + '==========-重启程序-==========')
            traceback.print_exc()
            print(time.ctime() + '==========-重启程序-==========')
            with open('./serialization.log') as f:
                for line in f:
                    _id = line.strip().split(':')[0]
                    _num = line.strip().split(':')[1]
                    flag = main_porcess(_id, _num)

