from pshc import PSHC
from pyspark import SparkContext, SparkConf, StorageLevel
from pyspark.sql import SparkSession
import abc_time
import datetime


def process(data):
    """
    处理 rdd，根据 rdd 里的 title,hAxisTextD,time 字段抽取出时间，并返回新的rdd
    :param data: rdd, 其中每个 row 格式为：
        {
            "id": "string",
            "title": "string",
            "hAxisTextD": "string",
            "last_updated": "string",
            "time": "string",
        }
    :return:
    """

    result = []

    for row in data:
        id = row['id']
        title = row['title'] if 'title' in row else ''  # 比如 ''
        axis = row['hAxisTextD'] if 'hAxisTextD' in row else '[]'  # 比如 '[]', '['2013', '2014']'
        last_updated = row['last_updated'] if 'last_updated' in row else ''  # 比如 '2018-01-12 13:01:21.675000'
        img_time = row['time'] if 'time' in row else None  # 比如 '2012-06-08 00:00:00'

        if isinstance(img_time, str):
            try:
                img_publish_year = datetime.datetime.strptime(img_time, '%Y-%m-%d %H:%M:%S').year
            except ValueError:
                img_publish_year = None
        else:
            img_publish_year = None

        if isinstance(title, str) and isinstance(axis, str) and type(eval(axis)) is list:

            # axis 可能是诸如这种格式的： ['2013', '2014']
            if all(isinstance(i, str) for i in eval(axis)):
                years = abc_time.ABCYear.extract(title, eval(axis), current_year=img_publish_year)

            # axis 也可能是诸如这种格式的： [{'text': '2013', 'rotated': False, 'tag': 'hAxisTextD',
            # 'bbox': {'xmin': 829, 'ymin': 750, 'ymax': 786, 'xmax': 984}, 'type': 'text', 'isTime': True},
            # {'text': '2014', 'rotated': False, 'tag': 'hAxisTextD',
            # 'bbox': {'xmin': 572, 'ymin': 750, 'ymax': 787, 'xmax': 725}, 'type': 'text', 'isTime': True}]
            if all(isinstance(i, dict) for i in eval(axis)):
                new_axis = [i['text'] for i in eval(axis)]
                years = abc_time.ABCYear.extract(title, new_axis, current_year=img_publish_year)

            # 将所在年份也加入进去
            if img_publish_year is not None and str(img_publish_year) not in years:
                years.append(str(img_publish_year))
                years.sort()
        else:
            years = []

        entry = dict()
        entry['id'] = str(id)
        entry['years'] = str(years)
        entry['years_update_time'] = str(last_updated)
        result.append(entry)

    return result


if __name__ == '__main__':

    conf = SparkConf().setAppName("TimeExtract")
    sc = SparkContext(conf=conf)
    spark_session = SparkSession(sc)
    connector = PSHC(sc, SparkSession)

    hb_charts_catelog = {
        "table": {"namespace": "default", "name": "hb_charts"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "title": {"cf": "data", "col": "title", "type": "string"},
            "hAxisTextD": {"cf": "data", "col": "hAxisTextD", "type": "string"},
            "fileId": {"cf": "data", "col": "fileId", "type": "string"},
            "last_updated": {"cf": "data", "col": "last_updated", "type": "string"},
        }
    }

    hibor_catelog = {
        "table": {"namespace": "default", "name": "hibor"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "time": {"cf": "data", "col": "title", "type": "string"},
        }
    }

    join_catelog = {
        "table": {"namespace": "default", "name": "hb_charts"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "years": {"cf": "data", "col": "years", "type": "string"},
            "years_update_time": {"cf": "data", "col": "years_update_time", "type": "string"},
        }
    }

    hb_charts_df = connector.get_df_from_hbase(hb_charts_catelog)
    hibor_df = connector.get_df_from_hbase(hibor_catelog)
    hb_charts_df.show()
    print('=========>', hb_charts_df.count())
    hibor_df.show()
    print('=========>', hibor_df.count())

    hb_charts_df.registerTempTable('table_hb_charts')
    hibor_df.registerTempTable('table_hibor')

    join_df = spark_session.sql("SELECT table_hb_charts.id, table_hb_charts.title, table_hb_charts.hAxisTextD, "
                                "table_hb_charts.last_updated, table_hibor.time FROM table_hb_charts JOIN table_hibor "
                                "ON table_hb_charts.fileId = table_hibor.id")

    join_df.filter("id IS NOT null AND title IS NOT null AND hAxisTextD IS NOT null AND "
                   "last_updated IS NOT null AND time IS NOT null")
    join_df.show()
    print('=========>', join_df.count())

    result_rdd = join_df.rdd.mapPartitions(lambda x: process(x)).persist(StorageLevel.DISK_ONLY)
    result_df = spark_session.createDataFrame(result_rdd, connector.catelog_to_schema(join_catelog))
    result_df.show()
    print('=========>', result_df.count())

    connector.save_df_to_hbase(result_df, join_catelog)
