from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import hashlib
import configparser
import logging

CONFIG_FILE = "path.conf"


class HbaseControl(object):

    def __init__(self, table, column_families, put_num=0):
        """
        初始化一个 HBase Table
        :param table: 表的名字,比如 b'hb_charts'
        :param column_families: 表的列族,比如 [b'cf1', b'cf2']
        :param host: host地址
        :param port: thrift服务端口
        """

        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)

        self.host = conf.get("hbase", "host")
        self.port = int(conf.get("hbase", "port"))

        # Connect to HBase Thrift server
        self.transport = TTransport.TBufferedTransport(TSocket.TSocket(self.host, self.port))
        self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)

        # Create and open the client connection
        self.client = Hbase.Client(self.protocol)
        self.transport.open()

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

    def puts(self, records, key):
        """
        hbase批量插入
        :param records: 多条条记录list，一条记录格式为{'_id':'','field1':'', 'field2':''}
        :param key: 每条记录的唯一标识的字段名，对于 MongoDB 是 '_id', 对于 MySQL 是 'id'
        :return: 
        """
        assert isinstance(records, list)

        mutations_batch = []
        for record in records:
            mutations = []
            # row_key的值为 md5(_id)[0:10]:_id
            _id = str(record[key])
            row_key = bytes(hashlib.md5(bytes(_id, encoding="utf-8")).hexdigest()[0:10] + ':' + _id, encoding="utf-8")
            for item in record:
                if item == key:
                    continue
                mutations.append(Hbase.Mutation(column=bytes('data:' + item, encoding="utf8"),
                                                value=bytes(str(record[item]), encoding="utf8")))
            mutations_batch.append(Hbase.BatchMutation(row=row_key, mutations=mutations))

        self.client.mutateRows(self.table, mutations_batch, {})
        self.last_id = str(records[-1])
        self.put_num += len(mutations_batch)
        serialization_handler = open('./serialization.log', 'w')
        print(self.last_id, file=serialization_handler)
        print(str(self.put_num), file=serialization_handler)
        serialization_handler.close()

