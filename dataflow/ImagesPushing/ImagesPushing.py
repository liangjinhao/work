from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import time
import datetime
import pshc
import requests
import hashlib


"""
该脚本采用Spark读取HBase的img_data表里的数据，并通过POST请求发送到Solr服务上去
"""

POST_URL = 'http://10.24.235.15:8080/solrweb/chartIndexByUpdate'  # Solr接受post请求的地址


def send(x):

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
            "type": "新闻资讯",  # 固定值, '新闻资讯'
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
            "doc_feature": "",  # 图片的特征，现在取的是直接算图片image_title的md5值，后期可能会变动
            "tags": "",
            "language": "1"  # 中文为1，英文为2
        }

        row_key = row['id']
        img_oss = row['img_oss']
        img_title = row['img_title'] if 'img_title' in row else ''
        img_type = row['img_type'] if 'img_type' in row else '[]'
        title = row['title'] if 'title' in row else ''
        url = row['url'] if 'url' in row else ''

        if img_title is None:
            img_title = ''
        if img_type is None:
            img_type = '[]'
        if title is None:
            title = ''
        if url is None:
            url = ''

        img_json['id'] = 'ggg' + row_key + '_1'
        img_json['image_id'] = 'ggg' + row_key
        img_json['image_title'] = img_title
        img_json['image_url'] = img_oss.replace('bj-image.oss-cn-hangzhou-internal.aliyuncs.com',
                                                'bj-image.oss-cn-hangzhou.aliyuncs.com')
        img_json['source_url'] = url

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

        if row['publish_time'] is not None:
            img_datetime = datetime.datetime.strptime(row['publish_time'], '%Y-%m-%d %H:%M:%S')
            img_json['time'] = int(time.mktime(img_datetime.timetuple()))
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
        img_json['doc_feature'] = hashlib.md5(bytes(img_title, 'utf-8')).hexdigest() if img_title != '' else ''

        # 累计300条post一次数据，不要累计太多，Solr端的Post请求有长度限制
        if post_count < 300:
            if img_json['image_title'] != '':
                post_imgs.append(img_json)
                post_count += 1
        else:
            requests.post(POST_URL, json=post_imgs)
            post_imgs = []
            post_count = 0

    requests.post(POST_URL, json=post_imgs)


if __name__ == '__main__':

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
            "publish_time": {"cf": "info", "col": "publish_time", "type": "string"},
        }
    }

    startTime = datetime.datetime.strptime('2018-1-31 11:59:59', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    stopTime = datetime.datetime.strptime('2018-05-01 1:0:0', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'

    df = connector.get_df_from_hbase(catelog, start_row=None, stop_row=None, start_time=startTime, stop_time=stopTime,
                                     repartition_num=None, cached=True)
    df.show(10)

    # tmp_rdd = df.rdd
    # countNum = tmp_rdd.count()
    # partitionNum = int(countNum / 10000 + 1)
    # rdd = tmp_rdd.repartition(partitionNum).cache()

    print('======count=======', df.count())
    df.rdd.foreachPartition(lambda x: send(x))
