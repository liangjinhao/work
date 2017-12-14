from pyspark import *
from pyspark.sql import *


def process(data):
    columns = [b'data:title', b'data:hAxisTextD']
    util = Utility()
    final = []
    for item in data:
        rowkey = item[b'rowKey']
        title = item[columns[0]].decode() if columns[0] in item else ''
        axis = item[columns[1]].decode() if columns[1] in item else []
        prob = util.get_probability(title, axis)
        years = util.extract_time(title, axis)
        entry = dict()
        entry[b'rowKey'] = rowkey
        entry[b'data:time_prob'] = bytes(str(prob), encoding='utf-8')
        entry[b'data:time_years'] = bytes(str(years), encoding='utf-8')
        final.append(entry)
    return iter(final)


if __name__ == "__main__":

    sc = SparkContext()

    from Utility import *
    import HBase_Tool

    spark = SparkSession(sc)
    tableName = b"hb_charts"
    masterIp = "slave1"
    port = 9090
    colnames = [b'data:title', b'data:hAxisTextD']
    hbase_tool = HBase_Tool.HBase_Tool(masterIp, port)
    rdd = hbase_tool.getHBaseTableAsRDD(tableName, sc, colnames)
    countNum = rdd.count()
    rdd.repartition(int(countNum/3000) + 1).persist(StorageLevel.DISK_ONLY)
    result_RDD = rdd.mapPartitions(lambda x: process(x))
    hbase_tool.outPutRDDToHBase(result_RDD, [b'data:time_prob', b'data:time_years'], b"hb_charts").count()
