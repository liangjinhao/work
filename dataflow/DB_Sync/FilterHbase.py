from pyspark import SparkConf, StorageLevel
from pyspark.sql import SparkSession
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
from pymongo import MongoClient

"""
删除 HBase 中已经标记为删除状态的数据以及在 MongoDB 中已经不存在的数据
"""

QUERY_MONGODB = True


def filter_data(x):
    host = '10.27.71.108'
    port = 9099
    transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    transport.open()

    user = 'search'
    password = 'ba3Re3ame+Wa'
    host = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
    port = 3717
    db_name = 'cr_data'
    table = 'hb_charts'

    mongo_client = MongoClient(host, port)
    db = mongo_client[db_name]
    db.authenticate(user, password)
    collection = db[table]

    for row in x:
        rowkey = row['key'].split(':')[-1]
        state = row['state']
        if QUERY_MONGODB:
            if state in ['3', '4', '5'] or collection.find({'_id': rowkey}).count() == 0:
                try:
                    client.deleteAllRow(b'hb_charts', bytes(row['key'], 'utf-8'), attributes=None)
                except:
                    pass
        else:
            if state in ['3', '4', '5']:
                try:
                    client.deleteAllRow(b'hb_charts', bytes(row['key'], 'utf-8'), attributes=None)
                except:
                    pass


if __name__ == '__main__':
    conf = SparkConf().setAppName("Filter_Hbase")
    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()

    df = sparkSession.sql('SELECT key, state FROM abc.hb_charts').persist(storageLevel=StorageLevel.DISK_ONLY)
    df.show(10)
    df.rdd.foreachPartition(lambda x: filter_data(x))
