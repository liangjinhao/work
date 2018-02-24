from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
import pshc
import Utils
import requests

"""
该脚本采用Spark读取HBase的news_data表里的数据，并通过POST请求发送到Solr服务上去

local mode:
spark-submit --master local --driver-memory 4G --executor-cores 2 --num-executors 4 
--packages 
org.apache.hbase:hbase:1.1.12,
org.apache.hbase:hbase-server:1.1.12,
org.apache.hbase:hbase-client:1.1.12,
org.apache.hbase:hbase-common:1.1.12
--jars 
/mnt/disk1/data/chyan/work/data_flow/spark-examples_2.10-1.6.4-SNAPSHOT.jar 
--conf spark.pyspark.python=/mnt/disk1/data/chyan/virtualenv/bin/python 
--py-files /mnt/disk1/data/chyan/work/data_flow/pshc.py /mnt/disk1/data/chyan/work/data_flow/NewsPushing.py

yarn mode:
spark-submit --master yarn --executor-memory 4G --executor-cores 2 --num-executors 4 
--packages 
org.apache.hbase:hbase:1.1.12,
org.apache.hbase:hbase-server:1.1.12,
org.apache.hbase:hbase-client:1.1.12,
org.apache.hbase:hbase-common:1.1.12
--jars 
/mnt/disk1/data/chyan/work/data_flow/spark-examples_2.10-1.6.4-SNAPSHOT.jar 
--conf spark.pyspark.python=/mnt/disk1/data/chyan/virtualenv/bin/python 
--py-files /mnt/disk1/data/chyan/work/data_flow/pshc.py /mnt/disk1/data/chyan/work/data_flow/NewsPushing.py
"""


def send(x):

    for row in x:

        news_json = dict({
            "id": "id",
            "author": "",  # author
            "channel": "",  # 首页 新闻中心 新闻
            "contain_image": "",  # False
            "content": "",
            "crawl_time": "",  # 2017-12-27 16:01:23
            "brief": "",  # dese
            "source_url": "",  # laiyuan
            "publish_time": "",  # 2017-12-01 10:20:49
            "source_name": "",  # source
            "title": "",  # title
            "url": "",  # url
            "tags": "",
            'doc_score': 1.0,
            "time": 0,
        })

        news_json['id'] = row['id']
        news_json['author'] = row['author']

        news_json['author'] = Utils.author_norm(row['author']) \
            if row['author'] is not None else row['author']

        news_json['channel'] = row['channel']
        news_json['contain_image'] = row['contain_image']

        news_json['content'] = Utils.content_norm(row['content']) \
            if row['content'] is not None else row['content']

        news_json['crawl_time'] = row['crawl_time']
        news_json['brief'] = row['dese']
        news_json['source_url'] = row['laiyuan']

        news_json['publish_time'] = Utils.time_norm(row['publish_time'])

        news_json['source_name'] = row['source']
        news_json['title'] = row['title']
        news_json['url'] = row['url']

        if news_json['publish_time'] is not None and news_json['publish_time'] != '':
            news_json['time'] = int(datetime.datetime.strptime(news_json['publish_time'], '%Y-%m-%d %H:%M:%S')
                                    .strftime('%s'))
        else:
            news_json['time'] = 0

        requests.post('http://10.168.20.246:8080/solrweb/indexByUpdate?single=true&core_name=core_news', json=[news_json])


if __name__ == '__main__':
    conf = SparkConf().setAppName("Push_News")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    connector = pshc.PSHC(sc, sqlContext)

    catelog = {
        "table": {"namespace": "default", "name": "news_data"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "author": {"cf": "info", "col": "author", "type": "string"},
            "channel": {"cf": "info", "col": "channel", "type": "string"},
            "contain_image": {"cf": "info", "col": "contain_image", "type": "string"},
            "content": {"cf": "info", "col": "content", "type": "string"},
            "crawl_time": {"cf": "info", "col": "crawl_time", "type": "string"},
            "dese": {"cf": "info", "col": "dese", "type": "string"},
            "laiyuan": {"cf": "info", "col": "laiyuan", "type": "string"},
            "publish_time": {"cf": "info", "col": "publish_time", "type": "string"},
            "source": {"cf": "info", "col": "source", "type": "string"},
            "title": {"cf": "info", "col": "title", "type": "string"},
            "url": {"cf": "info", "col": "url", "type": "string"},
        }
    }

    startTime = datetime.datetime.strptime('2018-1-31 11:59:59', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    stopTime = datetime.datetime.strptime('2018-05-01 1:0:0', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'

    df = connector.get_df_from_hbase(catelog, start_row=None, stop_row=None, start_time=startTime, stop_time=stopTime,
                                     repartition_num=None, cached=True)
    df.show(10)
    print('======count=======', df.count())
    df.rdd.foreachPartition(lambda x: send(x))
