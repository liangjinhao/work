from pyspark import *
from pyspark.sql import *
import abc_time


def process(data):

    columns = ['data:title', 'data:hAxisTextD', 'data:last_updated', 'data:time']
    final = []
    for item in data:

        rowkey = item['rowKey']
        title = item[columns[0]] if columns[0] in item else ''
        axis = item[columns[1]] if columns[1] in item else '[]'
        last_updated = item[columns[2]] if columns[2] in item else ''
        current_year = int(item[columns[3]][0:4]) if columns[3] in item else None

        if isinstance(title, str) and isinstance(axis, str) and type(eval(axis)) is list:

            # axis 可能是诸如这种格式的： ['2013', '2014']
            if all(isinstance(i, str) for i in eval(axis)):
                years = abc_time.ABCYear.extract(title, eval(axis), current_year=current_year)

            # axis 也可能是诸如这种格式的： [{'text': '2013', 'rotated': False, 'tag': 'hAxisTextD',
            # 'bbox': {'xmin': 829, 'ymin': 750, 'ymax': 786, 'xmax': 984}, 'type': 'text', 'isTime': True},
            # {'text': '2014', 'rotated': False, 'tag': 'hAxisTextD',
            # 'bbox': {'xmin': 572, 'ymin': 750, 'ymax': 787, 'xmax': 725}, 'type': 'text', 'isTime': True}]
            if all(isinstance(i, dict) for i in eval(axis)):
                new_axis = [i['text'] for i in eval(axis)]
                years = abc_time.ABCYear.extract(title, new_axis, current_year=current_year)

            # 将所在年份也加入进去
            if current_year is not None and str(current_year) not in years:
                years.append(str(current_year))
                years.sort()
        else:
            print(item)
            years = []

        entry = dict()
        entry['rowKey'] = rowkey
        entry['data:title'] = title
        entry['data:axis'] = axis
        entry['data:years'] = str(years)
        entry['data:years_update_time'] = last_updated
        final.append(entry)

    return iter(final)


if __name__ == "__main__":

    sc = SparkContext()

    from Utility import *
    import HBaseTool

    spark = SparkSession(sc)

    # 读取 hb_charts 生成DataFrame

    masterIp = "10.27.71.108"
    port = 9099
    hbase_tool = HBaseTool.HBaseTool(masterIp, port)

    hb_charts_table = b"hb_charts"
    hb_charts_columns = [b'data:title', b'data:hAxisTextD', b'data:fileId', b'data:last_updated']
    rdd_hb_charts = hbase_tool.get_rdd(hb_charts_table, sc, hb_charts_columns).persist(StorageLevel.DISK_ONLY)
    print('hb_charts rdd count *****', rdd_hb_charts.count())

    df_hb_charts = rdd_hb_charts.toDF()
    df_hb_charts.show()
    df_hb_charts.registerTempTable('df_hb_charts')

    hibor_table = b'hibor'
    hibor_columns = [b'data:time']
    rdd_hibor = hbase_tool.get_rdd(hibor_table, sc, hibor_columns).persist(StorageLevel.DISK_ONLY)
    print('hibor rdd counr *****', rdd_hibor.count())

    rdd_hibor = rdd_hibor.map(lambda x: {'rowKey': x['rowKey'].split(':')[-1], 'data:time': x['data:time']})

    df_hibor = rdd_hibor.toDF()
    df_hibor.show()
    df_hibor.registerTempTable('df_hibor')

    new_df = spark.sql("SELECT df_hb_charts.rowKey, df_hb_charts.`data:title`, df_hb_charts.`data:hAxisTextD`, "
                       "df_hb_charts.`data:last_updated`, df_hibor.`data:time` FROM df_hb_charts JOIN df_hibor "
                       "ON df_hb_charts.`data:fileId` != df_hibor.rowKey")

    new_df.filter("rowKey IS NOT null AND `data:title` IS NOT null AND `data:hAxisTextD` IS NOT null AND "
                  "`data:last_updated` IS NOT null AND `data:time` IS NOT null").persist(StorageLevel.DISK_ONLY)
    print('*******************', new_df.count())
    new_df.show()

    result_RDD = new_df.rdd.mapPartitions(lambda x: process(x)).persist(StorageLevel.DISK_ONLY)
    print('------------------', result_RDD.count())
    result_RDD.first()
    hbase_tool.write_rdd_to_hbase(result_RDD, ['data:title', 'data:axis', 'data:years', 'data:years_update_time'],
                                  "test").count()
