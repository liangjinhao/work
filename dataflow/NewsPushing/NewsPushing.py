from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import datetime
import pshc
import Utils
import requests
import re

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


def classify_news(news):
    line = ['tag:' + news[1], 'title:' + news[2]]
    ress = ["content" + "".join(re.split(u'[^\u4e00-\u9fa5]+', (news[2] + news[3])))]
    line.append(ress)
    label = []
    channel0 = [u'宏观', u'经济评述', u'税务总局', u'政策解读', u'深度分析']
    for j in channel0:
        if j in news[0] or j in news[1]:
            label.append(u'宏观')
            break
    channel1 = [u'股市', u'股票', u'证券', u'券商', u'港股', u'A股', u'大盘', u'美股', u'IPO', u'沪指', u'上证', u'新股', u'爱炒股', u'股民',
                u'机构看市', u'黑马', u'创业板', u'证监会', u'概念股', u'逆市']
    for j in channel1:
        if (j in news[0].replace(u'中国证券网', '').replace(u'上海证券通', '').replace(u'证券时报网', '')
                .replace(u'证券日报', '').replace(u'证券市场红周刊', '').replace(u'证券之星', '')
                or j in news[1] or j in news[2]) and u'证券要闻' not in news[0]:
            label.append(u'股市')
            break
    channel2 = [u'债市', u'债券', u'国债', u'美债']
    for j in channel2:
        if (j in news[0].replace(u'国债期货', '') or j in news[1]) and u'论坛' not in news[0]:
            label.append(u'债市')
            break

    channel3 = [u'商品', u'黄金', u'原油', u'白银', u'贵金属', u'现货']
    for j in channel3:
        if j in news[0] or j in news[1] or j in news[2]:
            label.append(u'商品')
            break
    channel4 = [u'期货']
    for j in channel4:
        if j in news[0] or j in news[1]:
            label.append(u'期货')
            break
    channel5 = [u'贵金属', u'黄金', u'白银', u'铂', u'有色']
    for j in channel5:
        if j in news[0] or j in news[1] or j in news[2]:
            label.append(u'贵金属')
            break
    channel6 = [u'基金', u'理财', u'网贷']
    for j in channel6:
        if j in news[0] or j in news[1] or j in news[2]:
            label.append(u'基金')
            break
    channel7 = [u'外汇', u'人民币', u'汇率', u'美元', u'欧元']
    for j in channel7:
        if j in news[0] or j in news[1] or j in news[2]:
            label.append(u'外汇')
            break
    channel8 = [u'行业', u'汽车', u'地产', u'楼市', u'保险', u'信托', u'轿车']
    for j in channel8:
        if (j in news[0] or j in news[1]) and u'中国证券网' not in news[0] and u'中国' not in news[2]:
            label.append(u'行业')
            break
    channel9 = [u'公司', u'机构', u'三板', u'万科']
    for j in channel9:
        if j in news[0] or j in news[1] or j in news[2]:
            label.append(u'公司')
            break
    channel10 = [u'科技', u'极客公园', u'雷锋网', u'钛媒体', u'金色财经', u'巴比特', u'区块链', u'AI', u'人工智能']
    for j in channel10:
        if (j in news[0] or j in news[1]) and u'证券' not in news[0] and u'爱站' not in news[1] \
                and u'公司' not in news[0]:
            label.append(u'科技')
            break
    channel11 = [u'观点', u'人物', u'专家', u'视点', u'凤凰财经研究院', u'大咖说', u'大家谈', u'WE言堂', u'社论', u'观察', u'名家专栏', u'见解']
    for j in channel11:
        if j in news[0] or j in news[1]:
            label.append(u'观点')
            break
    channel12 = [u'全球', u'国际', u'世界', u'天下', u'外交部', u'美国', u'环球', u'特朗普', u'政府', u'国籍']
    for j in channel12:
        if (j in news[0].replace(u'环球外汇', '') or j in news[2] or j in news[3]) \
                and u'银行首页' not in news[0] and u'证券' not in news[0] and u'财经世界' not in news[0] \
                and u'全球金融市场' not in news[0]:
            label.append(u'全球')
            break
    line.insert(0, label)
    if not line[0]:
        line[0].append(u'其他')
    return " ".join(line[0])


def get_category(data):
    try:
        if "channel" in data and "tag" in data and "title" in data and "content" in data and \
                data["channel"] and data["tag"] and data["title"] and data["content"]:
            label_data = [data["channel"], data["tag"], data["title"], data["content"]]
            label = classify_news(label_data)
        else:
            label = ""
    except Exception as e:
        print(e)
        label = ""
    return label


def send(x):

    for row in x:

        news_json = dict({
            "id": "id",
            "author": "",  # author
            "category": "",  # 新闻类型，比如'全球'，'行业'，'股票'
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
            "tag": "",
            'doc_score': 1.0,
            "time": 0,
        })

        news_json['id'] = row['id']
        news_json['author'] = row['author']

        news_json['author'] = Utils.author_norm(row['author']) \
            if row['author'] is not None else row['author']

        news_json['category'] = row['category'] if 'category' in row else ''

        if news_json['category'] == '':
            category = get_category(row)
            news_json['category'] = category if category != '' else ''

        news_json['channel'] = row['channel']
        news_json['contain_image'] = row['contain_image']

        news_json['content'] = Utils.content_norm(row['content']) \
            if row['content'] is not None else row['content']

        news_json['crawl_time'] = row['crawl_time']
        news_json['brief'] = row['dese']
        news_json['source_url'] = row['laiyuan']
        news_json['source_name'] = row['source']
        news_json['title'] = row['title']
        news_json['url'] = row['url']
        news_json['tag'] = row['tag'] if 'tag' in row else ''

        try:
            news_json['time'] = int(datetime.datetime.strptime(row['publish_time'], '%Y-%m-%d %H:%M:%S')
                                    .strftime('%s'))
            news_json['publish_time'] = row['publish_time']
        except:
            try:
                t = Utils.time_norm(news_json['publish_time'])
                news_json['time'] = int(datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S').strftime('%s'))
                news_json['publish_time'] = t
            except:
                news_json['publish_time'] = str(datetime.datetime.utcfromtimestamp(0))
                news_json['time'] = 0

        print(news_json)

        try:
            requests.post('http://10.168.20.246:8080/solrweb/indexByUpdate?single=true&core_name=core_news',
                          json=[news_json])
        except Exception as e:
            print(e)


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
            "tag": {"cf": "info", "col": "tag", "type": "string"},
            "category": {"cf": "info", "col": "category", "type": "string"},
        }
    }

    startTime = datetime.datetime.strptime('2018-1-31 11:59:59', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    stopTime = datetime.datetime.strptime('2018-05-01 1:0:0', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'

    df = connector.get_df_from_hbase(catelog, start_row=None, stop_row=None, start_time=startTime, stop_time=stopTime,
                                     repartition_num=None, cached=True)
    df.show(10)
    print('======count=======', df.count())
    df.rdd.foreachPartition(lambda x: send(x))
