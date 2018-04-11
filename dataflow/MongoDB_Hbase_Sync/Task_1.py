from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import pshc
import oss2
import logging
from hbase.ttypes import *


"""
该脚本读取 hb_text 表里的 text_file 字段（存有 OSS 链接）, 是然后取oss文本文件，再把oss 文本文件给写到 Hive 中
"""


# OSS 信息
ACCESS_KEY_ID = 'LTAIxqZZWfBv6jBf'
ACCESS_KEY_SECRET = 'NEQ966QOxCyXgpAyKkU6THUJ3xDxNV'
BUCKET_HZ = 'abc-crawler'
ENDPOINT_HZ = 'oss-cn-hangzhou-internal.aliyuncs.com'

# HBase Thrift 信息
THRIFT_IP = '10.27.71.108'
THRIFT_PORT = 9099


def save_to_hive(x):
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
                    parts = text.split('\n')
                    index = 1
                    for p in parts:
                        new_row = {
                            'id': row['id'],
                            'text': p,
                            'index': index
                        }
                        index += 1
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

    df = connector.get_df_from_hbase(catelog, repartition_num=1000).rdd.cache()
    print('======load file count=====', df.count())
    result_rdd = df.mapPartitions(lambda x: save_to_hive(x))
    result_df = spark_session.createDataFrame(result_rdd, ['id', 'text', 'index'])
    result_df.write.saveAsTable('abc.hb_text_oss_file', mode='overwrite')
    #print('======count=====', spark_session.table('abc.hb_text_oss_file').count())
