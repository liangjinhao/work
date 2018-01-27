from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import time
import datetime
import pshc
import requests


"""
local mode:
spark-submit --master local --driver-memory 4G --executor-cores 2 --num-executors 4 
--packages 
org.apache.hbase:hbase:1.1.12,
org.apache.hbase:hbase-server:1.1.12,
org.apache.hbase:hbase-client:1.1.12,
org.apache.hbase:hbase-common:1.1.12
--jars 
/mnt/disk1/data/chyan/work/post_image/spark-examples_2.10-1.6.4-SNAPSHOT.jar 
--conf spark.pyspark.python=/mnt/disk1/data/chyan/virtualenv/bin/python 
--py-files /mnt/disk1/data/chyan/work/post_image/pshc.py /mnt/disk1/data/chyan/work/post_image/send_image.py

yarn mode:
spark-submit --master yarn --executor-memory 4G --executor-cores 2 --num-executors 4 
--packages 
org.apache.hbase:hbase:1.1.12,
org.apache.hbase:hbase-server:1.1.12,
org.apache.hbase:hbase-client:1.1.12,
org.apache.hbase:hbase-common:1.1.12
--jars 
/mnt/disk1/data/chyan/work/post_image/spark-examples_2.10-1.6.4-SNAPSHOT.jar 
--conf spark.pyspark.python=/mnt/disk1/data/chyan/virtualenv/bin/python 
--py-files /mnt/disk1/data/chyan/work/post_image/pshc.py /mnt/disk1/data/chyan/work/post_image/send_image.py
"""


def send(x):

    result = []

    post_imgs = []
    post_count = 0

    for row in x:

        img_json = {
            "id": "",  # ggg + 图片url的md5 + _1
            "image_id": "",  # ggg + 图片url的md5
            "file_id": "",
            "image_title": "",  # 图片 title
            "image_legends": "",
            "image_url": "",  # 图片 oss_url
            "file_url": "",
            "source_url": "",  # 文章 url
            "chart_type": "",  # 图片类型, 比如: 'LINE_AREA', 'LINE_COLUMN'
            "bitmap_type": "",
            "type": "市场研报",  # 固定值, '市场研报'
            "publish": "",  # 网站 url
            "company": "",
            "author": "",
            "industry": "",
            "summary": "",
            "chart_data": "",

            "confidence": 1.0,
            "time": 0,  # 图的时间
            "year": 0,  # 图的年份
            "doc_score": 1.0,  # 默认填写1, 如果图片类型是需要的置 1.0，其余的则置 0.001
            "stockcode": "",
            "title": "",  # 文章 title
            "chart_version": 1,
            "doc_feature": "",
            "tags": "",
            "language": "1"  # 中文为1，英文为2
        }

        row_key = row['id']
        img_oss = row['img_oss']
        img_title = row['img_title'] if 'img_title' in row else ''
        img_type = row['img_type'] if 'img_type' in row else ''
        title = row['title'] if 'title' in row else ''
        url = row['url'] if 'url' in row else ''
        index_state = row['index_state'] if 'index_state' in row else '0'

        img_json['id'] = 'ggg' + row_key + '_1'
        img_json['image_id'] = 'ggg' + row_key
        img_json['image_title'] = img_title
        img_json['image_url'] = img_oss.replace('bj-image.oss-cn-hangzhou-internal.aliyuncs.com',
                                                'bj-image.oss-cn-hangzhou.aliyuncs.com')
        img_json['source_url'] = url

        if img_type is None:
            img_type = '[]'
        img_type = eval(img_type)
        if img_type:
            img_types = []
            [img_types.append(i['type']) for i in img_type]
            img_types.sort(reverse=True)

            if len(img_types) == 1:
                img_json['chart_type'] = img_types[0]
            else:
                img_json['chart_type'] = '_AND_'.join(img_types)
        else:
            img_json['chart_type'] = "OTHER"

        img_json['publish'] = url.lstrip('https?://').split('/')[0]

        img_time = '2018-01-01'
        img_datetime = datetime.datetime.strptime(img_time, '%Y-%m-%d')

        img_json['time'] = time.mktime(img_datetime.timetuple())
        img_json['year'] = img_datetime.year

        # 图片类型共有15种：  OTHER, OTHER_MEANINGFUL, AREA_CHART, BAR_CHART, CANDLESTICK_CHART, COLUMN_CHART,
        # LINE_CHART, PIE_CHART, LINE_CHART_AND_AREA_CHART, LINE_CHART_AND_COLUMN_CHART, GRID_TABLE, LINE_TABLE,
        # INFO_GRAPH, TEXT, QR_CODE
        if img_json['chart_type'] == "" or img_json['chart_type'] == "OTHER" or \
                img_json['chart_type'] == "OTHER_MEANINGFUL":
            img_json['doc_score'] = 0.01
        elif img_json['chart_type'] == "QR_CODE" or img_json['chart_type'] == "TEXT":
            img_json['doc_score'] = 0.1
        elif img_json['chart_type'] == "INFO_GRAPH":
            img_json['doc_score'] = 0.4
        else:
            img_json['doc_score'] = 0.7

        img_json['title'] = title

        if index_state != 1:

            # 累计1000条post一次数据
            if post_count < 1000:
                if img_json['title'] != '':
                    post_imgs.append(img_json)
                    post_count += 1
            else:
                requests.post('http://10.24.235.15:8080/solrweb/chartIndexByUpdate', json=post_imgs)
                post_imgs = []
                post_count = 0

            new_row = dict()
            new_row['id'] = row_key
            new_row['index_state'] = '1'

            result.append(new_row)

    requests.post('http://10.24.235.15:8080/solrweb/chartIndexByUpdate', json=post_imgs)

    return result


conf = SparkConf().setAppName("Send_Image")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
connector = pshc.PSHC(sc, sqlContext)

catelog = {
    "table": {"namespace": "default", "name": "img_data"},
    "rowkey": "id",
    "columns": {
        "id": {"cf": "rowkey", "col": "key", "type": "string"},
        "img_format": {"cf": "info", "col": "img_format", "type": "string"},
        "img_oss": {"cf": "info", "col": "img_oss", "type": "string"},
        "img_title": {"cf": "info", "col": "img_title", "type": "string"},
        "img_type": {"cf": "info", "col": "img_type", "type": "string"},
        "img_url": {"cf": "info", "col": "img_url", "type": "string"},
        "title": {"cf": "info", "col": "title", "type": "string"},
        "url": {"cf": "info", "col": "url", "type": "string"},
        "index_state": {"cf": "info", "col": "index_state", "type": "string"},
    }
}

df = connector.get_df_from_hbase(catelog)
df.show(10)
df.filter("index_state IS null OR index_state != 1")
df.show(10)

new_rdd = df.rdd.mapPartitions(lambda x: send(x))
new_df = sqlContext.createDataFrame(new_rdd, connector.catelog_to_schema(catelog))
new_df.show(10)
connector.save_df_to_hbase(new_df, catelog)
