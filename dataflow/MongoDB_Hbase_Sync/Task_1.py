from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import pshc
import oss2
import logging
import sys

"""
该脚本读取 hb_text 表里的 text_file 字段（存有 OSS 链接）, 是然后取oss文本文件，再把oss 文本文件给写到 Hbase 中
"""


# OSS 信息
ACCESS_KEY_ID = 'LTAIxqZZWfBv6jBf'
ACCESS_KEY_SECRET = 'NEQ966QOxCyXgpAyKkU6THUJ3xDxNV'
BUCKET_HZ = 'abc-crawler'
ENDPOINT_HZ = 'oss-cn-hangzhou-internal.aliyuncs.com'


def down_load_oss(x):

    bucket_hz = oss2.Bucket(oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET), ENDPOINT_HZ, BUCKET_HZ)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    logger = logging.getLogger('MyLogger')
    logger.addHandler(sh)

    result = []
    for row in x:
        if 'text_file' in row and row['text_file'] is not None:
            file_name = row['text_file'].split('aliyuncs.com/')[-1]
            if bucket_hz.object_exists(file_name):
                try:
                    file_stream = bucket_hz.get_object(file_name)
                    text = str(file_stream.read(), 'utf-8')
                    new_row = {
                        'id': row['id'],
                        'text': text
                    }
                    result.append(new_row)
                except Exception as e:
                    logging.error(e)
                    print(e)
                    continue

    return result


if __name__ == '__main__':

    conf = SparkConf().setAppName("Download_Hb_text")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark_session = SparkSession(sc)
    connector = pshc.PSHC(sc, sqlContext)

    catelog = {
        "table": {"namespace": "default", "name": "hb_text"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "text_file": {"cf": "data", "col": "text_file", "type": "string"},
        }
    }

    df = connector.get_df_from_hbase(catelog)
    df.show(50, False)
    print('======count=====', df.count())

    result_rdd = df.rdd.repartition(300).mapPartitions(lambda x: down_load_oss(x))
    result_df = spark_session.createDataFrame(result_rdd, ['id', 'text']).show

    result_df.show(50, False)
    print('======count=====', result_df.count())

    catelog2 = {
        "table": {"namespace": "default", "name": "hb_text_oss_file"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "text": {"cf": "data", "col": "text", "type": "string"}
        }
    }

    connector.save_df_to_hbase(result_df, catelog2)
