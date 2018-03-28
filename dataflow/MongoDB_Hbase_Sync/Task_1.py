from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import pshc
import oss2
import logging

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

    result = []
    for row in x:
        if 'text_file' in row and row['text_file'] is not None:
            file_name = row['text_file'].split('aliyuncs.com/')[-1]
            if bucket_hz.object_exists(file_name):
                try:
                    file_stream = bucket_hz.get_object(file_name)
                except oss2.exceptions.NoSuchKey as e:
                    logging.error(e)
                    print(e)
                    continue
                except Exception as e:
                    logging.error(e)
                    print(e)
                    continue
                text = ''

                is_binary = False
                LOGGER.info('Reading File: ' + row['text_file'])
                print('Reading File: ', row['text_file'])
                for line in file_stream:
                    if isinstance(line, bytes):
                        is_binary = True
                        decoded_line = str(line, 'utf-8')
                        text = text + decoded_line
                    else:
                        text = text + line
                if is_binary:
                    LOGGER.info('是一个二进制文件，使用 utf-8 编码后是\n' + text)
                    print('是一个二进制文件，使用 utf-8 编码后是\n', text)

                new_row = {
                    'id': row['id'],
                    'text': text
                }
                result.append(new_row)
    return result


if __name__ == '__main__':

    conf = SparkConf().setAppName("Download_Hb_text")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    spark_session = SparkSession(sc)
    connector = pshc.PSHC(sc, sqlContext)

    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    catelog = {
        "table": {"namespace": "default", "name": "hb_text"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "text_file": {"cf": "data", "col": "text_file", "type": "string"},
        }
    }

    df = connector.get_df_from_hbase(catelog)
    df.show(10)
    print('======count=====', df.count())

    result_rdd = df.rdd.mapPartitions(lambda x: down_load_oss(x))
    result_df = spark_session.createDataFrame(result_rdd, ['id', 'text'])

    result_df.show(10)
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
