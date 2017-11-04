# -*- coding: utf-8 -*-

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import hashlib
import time
import traceback
import logging

class HbaseControl(object):

    def __init__(self, table, column_families, host='121.199.7.171', port=9090, put_num=0):
        """
        初始化一个 HBase Table
        :param table: 表的名字,比如 b'hb_charts'
        :param column_families: 表的列族,比如 [b'cf1', b'cf2']
        :param host: host地址
        :param port: thrift服务端口
        """

        self.host = host
        self.port = port

        # Connect to HBase Thrift server
        tsocket = TSocket.TSocket(host, port)
        tsocket.setTimeout(None)
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        # Create and open the client connection
        self.client = Hbase.Client(self.protocol)
        self.transport.open()

        self.max_sleep_time = 60*60  # 最大等待时间为1小时
        self.last_id = ''
        self.put_num = put_num

        # set table and column families
        self.table = table
        self.columnFamilies = column_families
        tables = self.client.getTableNames()
        if self.table not in tables:
            cf = []
            for columnFamily in self.columnFamilies:
                name = Hbase.ColumnDescriptor(name=columnFamily)
                cf.append(name)
            self.client.createTable(table, cf)

        logging.basicConfig(level=logging.WARNING,
                            filename='./process.log',
                            filemode='w',
                            format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

    def __del__(self):
        """
        销毁对象前关闭hbase链接
        :return: 
        """
        self.transport.close()

    def puts(self, records, sleep_time=60):
        """
        hbase批量插入
        :param records: 多条条记录list，一条记录格式为{'data:_id':'','cf1:c1':'', 'cf2:c1':''}
        :param sleep_time: 第一次连接出现异常再次连接的间隔时间,单位为s
        :return: 
        """
        assert isinstance(records, list)

        mutations_batch = []
        for record in records:
            mutations = []
            # row_key的值为 md5(_id)[0:10]:_id
            _id = str(record['data:_id'])
            row_key = bytes(hashlib.md5(bytes(_id, encoding="utf-8")).hexdigest()[0:10] + ':' + _id, encoding="utf-8")
            for item in record:
                if item == 'data:_id':
                    continue
                mutations.append(Hbase.Mutation(column=bytes(item, encoding="utf8"),
                                                value=bytes(str(record[item]), encoding="utf8")))
            mutations_batch.append(Hbase.BatchMutation(row=row_key, mutations=mutations))
        try:
            self.client.mutateRows(self.table, mutations_batch, {})
            self.last_id = str(records[-1]['data:_id'])
            self.put_num += len(mutations_batch)
            serialization_handler = open('./serialization.log', 'w')
            print(self.last_id + ':' + str(self.put_num), file=serialization_handler)
            serialization_handler.close()
        except Exception:
            if sleep_time > self.max_sleep_time:
                logging.warning(traceback.format_exc())
                raise Exception('尝试了多次，仍然失败')
            logging.warning('Hbase Put失败，开始睡眠{0}后重试'.format(sleep_time/60))
            time.sleep(sleep_time)
            self.puts(records, sleep_time*2)

