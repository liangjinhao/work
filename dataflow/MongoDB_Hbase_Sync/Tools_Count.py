from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import datetime
import pshc

if __name__ == '__main__':

    conf = SparkConf().setAppName("Count_Tables")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')

    connector = pshc.PSHC(sc, sqlContext)

    startTime = datetime.datetime.strptime('2018-3-1 0:0:0', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    stopTime = datetime.datetime.strptime('2100-1-1 0:0:0', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    table_name = 'hb_text'

    catelog = {
        "table": {"namespace": "default", "name": table_name},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "create_time": {"cf": "data", "col": "create_time", "type": "string"}
        }
    }

    df = connector.get_df_from_hbase(catelog, start_row=None, stop_row=None, start_time=startTime,
                                     stop_time=stopTime,
                                     repartition_num=None, cached=True)

    print(table_name + ' 的条数为：', df.count())
