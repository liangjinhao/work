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

    var1 = input("Please enter Hbase table: ")
    var2 = input("Please enter start_time ('%Y-%m-%d %H:%M:%S'): ")
    var3 = input("Please enter stop_time ('%Y-%m-%d %H:%M:%S'): ")

    startTime = None
    stopTime = None
    if var2 is not None and var3 is not None:
        try:
            startTime = datetime.datetime.strptime(var2, '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
            stopTime = datetime.datetime.strptime(var3, '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
        except Exception:
            raise

    table_name = var1
    catelog = {
        "table": {"namespace": "default", "name": table_name},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"}
        }
    }

    df = connector.get_df_from_hbase(catelog, start_row=None, stop_row=None, start_time=startTime,
                                     stop_time=stopTime,
                                     repartition_num=None, cached=True)

    print(table_name + ' 的条数为：', df.count())
