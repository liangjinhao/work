from pyspark import *
from pyspark.sql import *
import abc_time


def process(data):

    columns = [b'data:title', b'data:hAxisTextD', b'data:last_updated', b'data:time']
    final = []
    for item in data:
        rowkey = item[b'rowKey']
        title = item[columns[0]].decode() if columns[0] in item else ''
        axis = item[columns[1]].decode() if columns[1] in item else []
        last_updated = item[columns[2]].decode() if columns[2] in item else ''
        current_year = item[columns[3]].decode() if columns[3] in item else ''

        years = abc_time.ABCYear.extract(title, axis, current_year=current_year)

        entry = dict()
        entry['rowKey'] = rowkey.decode('utf-8')
        entry['data:years'] = bytes(str(years), encoding='utf-8')
        entry['data:years_update_time'] = bytes(str(last_updated), encoding='utf-8')
        final.append(entry)
    return iter(final)


if __name__ == "__main__":

    sc = SparkContext()

    from Utility import *
    import HBaseTool

    spark = SparkSession(sc)

    # 读取 hb_charts 生成DataFrame

    masterIp = "10.24.242.138"
    port = 9090
    hbase_tool = HBaseTool.HBaseTool(masterIp, port)

    hb_charts_table = b"hb_charts"
    hb_charts_columns = [b'data:title', b'data:hAxisTextD', b'data:fileId', b'data:last_updated']
    rdd_hb_charts = hbase_tool.get_rdd(hb_charts_table, sc, hb_charts_columns)

    # countNum = rdd_hb_charts.count()
    # rdd_hb_charts.repartition(int(countNum/3000) + 1).persist(StorageLevel.DISK_ONLY)

    df_hb_charts = rdd_hb_charts.toDF()

    hibor_table = b'hibor'
    hibor_columns = [b'data:time']
    rdd_hibor = hbase_tool.get_rdd(hibor_table, sc, hibor_columns)
    rdd_hibor = rdd_hibor.map(lambda x: [x[0].split(':')[-1], x[1]])

    df_hibor = rdd_hibor.toDF()

    new_df = spark.sql("SELECT df_hb_charts.data:rowKey, df_hb_charts.data:title, df_hb_charts.data:hAxisTextD, "
                       "df_hb_charts.data:last_updated df_hibor.data:time FROM df_hb_charts, df_hibor "
                       "WHERE df_hb_charts.data:fileId=df_hibor.rowKey")

    result_RDD = new_df.rdd.mapPartitions(lambda x: process(x))
    hbase_tool.write_rdd_to_hbase(result_RDD, ['data:years', 'data:years_update_time'], "hb_charts").count()
