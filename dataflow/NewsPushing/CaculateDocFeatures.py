import hashlib
import datetime
import grpc
import re
import hanlp_pb2, hanlp_pb2_grpc
from pyspark import StorageLevel
import time
from pshc import PSHC
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


"""
0、程序为在spark上跑的代码
1、从title中取关键字和从content中取关键字
2、将关键字合并，并通过一个hash函数，映射为一个hash值
3、hash值相同的，认为文章相同，去掉
"""


# 读取文件并分词
def run(texts):
    # 建立链接
    _HOST = '10.168.20.246'
    _PORT = '50051'
    conn = grpc.insecure_channel(_HOST + ':' + _PORT)
    client = hanlp_pb2_grpc.GreeterStub(channel=conn)
    # 分词，返回结果
    lst_res = []
    for text in texts:
        try:
            _text = "".join(re.findall(r'[\u4e00-\u9fa5]+', text[0], re.S))
            response = client.segment(hanlp_pb2.HanlpRequest(text=_text, indexMode=0, nameRecognize=1, translatedNameRecognize=1))
            # TF词频
            dit = {}
            for term in response.data:
                if term.word != " " and term.word != ",":
                    if term.word not in dit:
                        dit[term.word] = 1
                    else:
                        dit[term.word] += 1
            # 返回结果：[key,value]
            lst = []
            for k, v in dit.items():
                lst.append([k, v])
            #[[k,v], title, url]
            lst_res.append([lst, text[1], text[2]])
        except:
            pass
    return lst_res


# title处理
def clean_title(title):
    rr1 = re.compile(r'([0-9]*)\.([0-9+]*)')
    rr2 = re.compile(r'([0-9]*)月|([0-9]*)日')
    title_clean = ''
    if rr1.findall(title):
        res1 = ["".join(ks) for ks in rr1.findall(title)]
        title_clean = "".join(res1)
    elif rr2.findall(title):
        res2 = ["".join(ks) for ks in rr2.findall(title)]
        title_clean = "".join(res2)
    return title_clean


if __name__ == '__main__':

    conf = SparkConf().setAppName("Fenci_org")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    spark_session = SparkSession(sc)
    connector = PSHC(sc, spark_session)
    catelog1 = {
        "table": {"namespace": "default", "name": "news_data"},
        "rowkey": "id",
        "domain": "",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "title": {"cf": "info", "col": "title", "type": "string"},
            "content": {"cf": "info", "col": "content", "type": "string"},
            "url": {"cf": "info", "col": "url", "type": "string"}
        }
    }

    # 设定一个时间截止日期,5天
    timeStamp = time.time() - 86400 * 1
    timeArray = time.localtime(timeStamp)
    start_times = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    end_times = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

    startTime = datetime.datetime.strptime(start_times, '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    stopTime = datetime.datetime.strptime(end_times, '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    df_1 = connector.get_df_from_hbase(catelog1, start_time=startTime, stop_time=stopTime)
    df_1.registerTempTable('table_test1')
    new_df = spark_session.sql("select table_test1.content,table_test1.title,table_test1.id from table_test1")
    org_data_rdd = new_df.rdd.filter(lambda x: x[0] is not None and x[1] is not None).repartition(100).persist(
        StorageLevel.DISK_ONLY)

    # content切词
    content_cut_rdd = org_data_rdd.mapPartitions(run)

    # 提取title中的日期
    title_clean_rdd = content_cut_rdd.map(lambda x: [x[0], clean_title(x[1]), x[2]])
    title_clean_exchange = title_clean_rdd.map(lambda x: [[kv[0], [kv[1], x[1], x[2]]]
                                                          for kv in x[0] if len(kv[0]) > 1]).flatMap(lambda x: x)

    idf_rdd = sc.textFile("hdfs://10.27.71.108:8020/spark_data/out/xlxu/idf_clean.txt")
    idf_kv = idf_rdd.map(lambda x: x.split(","))

    # 初步筛选用于计算tf_idf的数据
    # [(keyword ,([word_count,title_data or '',id], idf_value))]
    tf_idf = title_clean_exchange.leftOuterJoin(idf_kv).filter(lambda x: x[1][1] != None)
    # [id, [[keyword, weight, title_data]]]
    tf_idf_exchange = tf_idf.map(lambda x: [x[1][0][2], [[x[0], x[1][0][0] * eval(x[1][1]), x[1][0][1]]]])
    # [id,[[keyword1, weight1], [keyword2, weight2]]]
    tf_idf_reduce = tf_idf_exchange.reduceByKey(lambda x, y: x + y)
    # 取权重最高的8个
    tf_idf_top = tf_idf_reduce.map(lambda x: [x[0], sorted(x[1], key=lambda x: x[1], reverse=True)[:8]])
    # [id, title, [keyword1, keyword2, keyword3]]
    tf_idf_keyword = tf_idf_top.map(lambda x: [x[0], x[1][0][2], " ".join([y[0] for y in x[1]])])

    result_rdd = tf_idf_keyword.map(lambda x: [
        x[0],
        hashlib.md5(bytes(x[1] + " " + x[2], 'utf-8')).hexdigest(),
        x[2]
    ])

    result_df = result_rdd.toDF(['id', 'doc_feature', 'keywords'])
    result_category = {
        "table": {"namespace": "default", "name": "news_data"},
        "rowkey": "id",
        "columns": {
               "id": {"cf": "rowkey", "col": "key", "type": "string"},
               "keywords": {"cf": "info", "col": "keywords", "type": "string"},
               "doc_feature": {"cf": "info", "col": "doc_feature", "type": "string"},
        }
    }

    result_df.show()
    # connector.save_df_to_hbase(result_df, result_category)
