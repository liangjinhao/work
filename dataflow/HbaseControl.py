from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import hashlib
import configparser
import time

CONFIG_FILE = "path.conf"


class HbaseControl(object):

    def __init__(self, table, column_families, file_lock, put_num=0):
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

        self.file_lock = file_lock
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

    def __del__(self):
        """
        销毁对象前关闭hbase链接
        :return: 
        """
        self.transport.close()

    def puts(self, records, job_id):
        """
        hbase批量插入
        :param records: 多条条记录list，一条记录格式为{'_id':'','field1':'', 'field2':''}
        :param job_id: 任务类型，比如 'mongodb:hb_charts'
        :return: 
        """
        assert isinstance(records, list)

        row_name = ''  # 行的ID
        log_column = ''  # 记下的列，比如 update_at 列
        if job_id.split(':')[0] == 'mongodb':
            row_name = '_id'
            log_column = 'last_updated'
        elif job_id.split(':')[0] == 'mysql':
            row_name = 'id'
            log_column = 'update_at'

        mutations_batch = []
        for record in records:
            mutations = []
            # row_key的值为 md5(_id)[0:10]:_id
            _id = str(record[row_name])
            row_key = bytes(hashlib.md5(bytes(_id, encoding="utf-8")).hexdigest()[0:10] + ':' + _id, encoding="utf-8")
            for item in record:
                if item == row_name:
                    continue

                key = bytes('data:' + item, encoding="utf8")
                var = bytes(str(record[item]), encoding="utf8")
                # hbase.client.keyvalue.maxsize 默认是10M，超出这个值则设置为None
                if len(var) < 10 * 1024 * 1024:
                    mutations.append(Hbase.Mutation(column=key, value=var))
                else:
                    mutations.append(Hbase.Mutation(column=key, value=bytes(str(None), encoding="utf8")))

            mutations_batch.append(Hbase.BatchMutation(row=row_key, mutations=mutations))

        self.client.mutateRows(self.table, mutations_batch, {})

        self.put_num += len(mutations_batch)

        with self.file_lock:
            f = open(job_id + '.txt', 'w')
            json = dict({'date': '', 'job_id': '', 'id': '', 'update': '', 'number': ''})
            json['date'] = time.strftime('%Y-%m-%d %H:%M:%S')
            json['job_id'] = job_id
            json['id'] = records[-1][row_name]
            if job_id.split(':')[0] == 'mongodb':
                json['update'] = records[-1][log_column]
            elif job_id.split(':')[0] == 'mysql':
                json['update'] = records[-1][log_column].strftime('%Y-%m-%d %H:%M:%S')
            json['number'] = str(self.put_num)
            f.write(str(json))
            f.close()
