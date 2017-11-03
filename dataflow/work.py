# -*- coding: utf-8 -*-
import HbaseControl
import MongodbControl
import time
import logging
import traceback


if __name__ == '__main__':
    logging.basicConfig(level=logging.WARNING,
                        filename='./serialization.log',
                        filemode='w',
                        format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    f_log = open('./console.log', 'w+')

    hc = HbaseControl.HbaseControl(table=b'hb_charts', column_families=[b'data'])
    mc = MongodbControl.MongdbControl('hb_charts')
    records = []
    all_count = 0
    count = 0
    temp_time = time.time()
    try:
        for i in mc.yield_data():
            if count >= 500:
                # print('Get Mongo time: ', str(time.time() - temp_time))
                put_time = time.time()
                hc.puts(records)
                # print('Put Hbase time: ', str(time.time() - put_time))
                temp_time = time.time()
                records = []
                all_count += count
                count = 0

                if all_count % 10000 == 0:
                    print(time.ctime() + '  已经处理{0}万条数据！！'.format(all_count / 10000))
                    print(time.ctime() + '  已经处理{0}万条数据！！'.format(all_count / 10000), file=f_log)
            else:
                records.append(i)
                count += 1
    except Exception as e:
        logging.warning(hc.last_id)
        print(time.ctime() + '  ====出现异常！！====', file=f_log)
        traceback.print_exc()
        print(traceback.print_exc(), file=f_log)
