from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import datetime
import pshc
import Utils
import requests
import hashlib
import re
import ast
import json
import redis
import pymysql
import traceback
from ac_search import ACSearch
import site_rank


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

# 原来的资讯的推送地址 'http://10.165.101.72:8086/news_update'
# 新加的资讯的推送地址 'http://10.80.62.207:8080/onlySolr/core_news'
POST_URLS = ['http://10.80.62.207:8080/onlySolr/core_news/update?wt=json']
DereplicationRedis = [
    {
        'ip': '10.81.88.218',
        'port': 8103,
        'password': 'qQKQwjcB0bdqD',
        'setname': "solr2_latest_titles"
    }
]

# POST_URLS = ['http://10.165.101.72:8086/news_update']
# DereplicationRedis = [
#     {
#         'ip': '10.81.88.218',
#         'port': 8103,
#         'password': 'qQKQwjcB0bdqD',
#         'setname': "latest_titles"
#     }
# ]


class StockInformer:

    def __init__(self):
        self.stock_info_file = ''
        self.stock_info = self.update()

        self.ac = ACSearch()
        for i in self.stock_info:
            self.ac.add_word(i)
        self.ac.start()

    # 从线上MySQL数据库拉取股票代码，名称和行业等信息
    def update(self):

        host = '10.117.211.16'
        port = 6033
        user = 'stin_sys_ro_pe'
        password = 'b405038da87d'
        db = 'r_reportor'
        stock_info = {}
        try:
            connection = pymysql.connect(host=host, port=port, db=db,
                                         user=user, password=password, charset='utf8',
                                         cursorclass=pymysql.cursors.DictCursor)
            # 选取美股（GICS行业标准），港股（GICS行业标准），A股（申银行业标准）
            sql = "SELECT sec_basic_info.sec_code, sec_industry_new.stk_code, sec_basic_info.sec_name, " \
                  "sec_industry_new.second_indu_name FROM r_reportor.sec_basic_info join r_reportor.sec_industry_new " \
                  "WHERE sec_basic_info.sec_uni_code = sec_industry_new.sec_uni_code AND " \
                  "(indu_standard = '1001007' OR indu_standard = '1001016') AND sec_industry_new.if_performed = '1';"
            cursor = connection.cursor()
            cursor.execute(sql)
            for row in cursor:
                stock_code = row['sec_code']
                stk_code = row['stk_code']
                stock_name = row['sec_name']
                stock_industry = row['second_indu_name']
                if stk_code.endswith(".N") or stk_code.endswith(".O") or stk_code.endswith(".A"):
                    stock_info[stock_name] = (stock_code, stk_code, stock_name, stock_industry)
                else:
                    stock_info[stock_name] = (stock_code, stk_code, stock_name, stock_industry)
                    stock_info[stock_code] = (stock_code, stk_code, stock_name, stock_industry)

            return stock_info
        except Exception as e:
            raise e

    def extract_stock_info(self, text):
        matched_list = self.ac.search(text)
        matched_list = list(set(matched_list))
        result = {'stock_code': [], 'stock_name': [], 'stock_industry': []}
        for item in matched_list:
            if re.match('\d{4,}', item):  # 匹配到股票代码
                if self.stock_info[item][1] in result['stock_code']:
                    continue
                result['stock_name'].append(self.stock_info[item][2])
                result['stock_code'].append(self.stock_info[item][1])
                result['stock_industry'].append(self.stock_info[item][3])
            else:  # 匹配到股票名字
                if self.stock_info[item][2] in result['stock_name']:
                    continue
                result['stock_name'].append(self.stock_info[item][2])
                result['stock_code'].append(self.stock_info[item][1])
                result['stock_industry'].append(self.stock_info[item][3])
        return result


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

    result = []

    si = StockInformer()

    site_ranks = site_rank.site_ranks

    postData = []
    postSize = 100

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
            "doc_feature": "",  # 区分文档的特征，现为 title 的 md5 值
            "first_image_oss": "",  # 资讯的第一个图片oss链接
            "source_url": "",  # laiyuan
            "publish_time": "",  # 2017-12-01 10:20:49
            # "keywords": "",  # tf-idf最高的8个词
            "source_name": "",  # source
            "title": "",  # title
            "url": "",  # url
            "tags": "",
            'doc_score': 1.0,  # 新闻网站的PageRank值
            "time": 0,
            "stockcode": "",
            "stockname": "",
            "industryname": ""
        })

        news_json['id'] = row['id']
        news_json['author'] = row['author']

        news_json['author'] = Utils.author_norm(row['author']) \
            if row['author'] is not None else row['author']

        news_json['category'] = row['category'] if 'category' in row else ''

        if news_json['category'] == '':
            category = get_category(row)
            news_json['category'] = category if category != '' else '其他'

        news_json['channel'] = row['channel']
        news_json['contain_image'] = row['contain_image']

        news_json['content'] = Utils.content_norm(row['content']) \
            if row['content'] is not None else row['content']

        news_json['crawl_time'] = row['crawl_time']
        news_json['brief'] = row['dese']

        news_json['doc_feature'] = row['doc_feature'] if 'doc_feature' in row else ''

        if 'image_list' in row and row['image_list'] != '' and row['image_list'] != '[]' and row['image_list'] is not None:
            try:
                image_list = ast.literal_eval(row['image_list'])
                if isinstance(image_list, list):
                    news_json['first_image_oss'] = image_list[0].replace("-internal.aliyuncs.com", ".aliyuncs.com")
            except Exception as e:
                print(e)

        news_json['source_url'] = row['laiyuan']
        news_json['source_name'] = row['source']
        news_json['title'] = row['title']

        # 从 title 提取出股票相关信息
        if news_json['title'] != '' and news_json['title'] is not None:
            stock_info = si.extract_stock_info(news_json['title'])
            stock_pair = []
            for i in range(len(stock_info['stock_code'])):
                stock_pair.append([stock_info['stock_code'][i], stock_info['stock_name'][i]])
            news_json['stockcode'] = json.dumps(stock_pair) if stock_pair != [] else ''
            news_json['stockname'] = ','.join(stock_info['stock_name'])
            news_json['industryname'] = ','.join(stock_info['stock_industry'])

        news_json['url'] = row['url']
        news_json['tags'] = row['tag']

        if news_json['url'] is not None:
            domain = news_json['url'].replace('https://', '').replace('http://', '').replace('www.', '').split('/')
            if domain[0] in site_ranks:
                news_json['doc_score'] = site_ranks[domain[0]] if site_ranks[domain[0]] != 0.0 else 1.0

        try:
            # 时间早于 2000 年的不推送
            if row['publish_time'][0:4] < '2000':
                continue
            news_json['time'] = int(datetime.datetime.strptime(row['publish_time'], '%Y-%m-%d %H:%M:%S')
                                    .strftime('%s'))
            news_json['publish_time'] = row['publish_time']
        except:
            try:
                t = Utils.time_norm(news_json['publish_time'])
                news_json['time'] = int(datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S').strftime('%s'))
                news_json['publish_time'] = t
            except:
                continue
                # news_json['publish_time'] = str(datetime.datetime.utcfromtimestamp(0))
                # news_json['time'] = 0

        # 根据 Redis 中 Title 的缓存去重，选择是否进行推送
        if news_json['title']is not None:
            dr = DereplicationRedis[0]
            dp_redis = redis.Redis(host=dr['ip'], port=dr['port'], password=dr['password'])
            normed_title = "".join(re.findall("[0-9a-zA-Z\u4e00-\u9fa5]+", news_json['title']))
            title_hash = hashlib.md5(bytes(normed_title, 'utf-8')).hexdigest()
            if dp_redis.zscore(dr['setname'], title_hash):
                dp_redis.zadd(dr['setname'], title_hash, news_json['time'])
            else:
                dp_redis.zadd(dr['setname'], title_hash, news_json['time'])
                news_json['index_time'] = datetime.datetime.now().isoformat()
                postData.append(news_json)

        if len(postData) > postSize:
            try:
                for i in range(len(POST_URLS)):
                    head = {'Content-Type': 'application/json'}
                    params = {"overwrite": "true", "commitWithin": 100000}
                    url = POST_URLS[i]
                    r = requests.post(url, params=params, headers=head, json=postData)
                    message = r.text
                    if r.status_code != 200:
                        print("xxx>", r.status_code, r.text, len(postData), postData[0]['id'], postData[len(postData)-1]['id'])
                        result.append({"status": 0, "message": message})
                    else:
                        print("===>", r.status_code, len(postData), postData[0]['id'], postData[len(postData)-1]['id'])
                        result.append({"status": 1, "message": message})
                    postData = []
            except Exception as e:
                print("!!!>", traceback.format_exc())
                result.append({"status": 0, "message": traceback.format_exc()})

    try:
        for i in range(len(POST_URLS)):
            head = {'Content-Type': 'application/json'}
            params = {"overwrite": "true", "commitWithin": 100000}
            url = POST_URLS[i]
            r = requests.post(url, params=params, headers=head, json=postData)
            message = r.text
            if r.status_code != 200:
                print("xxx>", r.status_code, r.text, len(postData), postData[0]['id'], postData[len(postData)-1]['id'])
                result.append({"status": 0, "message": message})
            else:
                print("===>", r.status_code, len(postData), postData[0]['id'], postData[len(postData)-1]['id'])
                result.append({"status": 1, "message": message})
    except Exception as e:
        print("!!!>", traceback.format_exc())
        result.append({"status": 0, "message": traceback.format_exc()})

    return result


if __name__ == '__main__':
    conf = SparkConf().setAppName("Push_News")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')

    connector = pshc.PSHC(sc, sqlContext)

    catelog = {
        "table": {"namespace": "default", "name": "news_data"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "author": {"cf": "info", "col": "author", "type": "string"},
            "category": {"cf": "info", "col": "category", "type": "string"},
            "channel": {"cf": "info", "col": "channel", "type": "string"},
            "contain_image": {"cf": "info", "col": "contain_image", "type": "string"},
            "content": {"cf": "info", "col": "content", "type": "string"},
            "crawl_time": {"cf": "info", "col": "crawl_time", "type": "string"},
            "dese": {"cf": "info", "col": "dese", "type": "string"},
            "doc_feature": {"cf": "info", "col": "doc_feature", "type": "string"},
            "image_list": {"cf": "info", "col": "image_list", "type": "string"},
            "laiyuan": {"cf": "info", "col": "laiyuan", "type": "string"},
            "publish_time": {"cf": "info", "col": "publish_time", "type": "string"},
            "keywords": {"cf": "info", "col": "keywords", "type": "string"},
            "source": {"cf": "info", "col": "source", "type": "string"},
            "title": {"cf": "info", "col": "title", "type": "string"},
            "url": {"cf": "info", "col": "url", "type": "string"},
            "tag": {"cf": "info", "col": "tag", "type": "string"},
        }
    }

    # 指定选取的推送起始时间，格式为 '%Y-%m-%d %H:%M:%S'
    begin = '2018-05-01 0:0:0'

    startTime = datetime.datetime.strptime(begin, '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'
    stopTime = datetime.datetime.strptime('2050-01-01 1:0:0', '%Y-%m-%d %H:%M:%S').strftime('%s') + '000'

    df = connector.get_df_from_hbase(catelog, start_row=None, stop_row=None, start_time=startTime,
                                     stop_time=stopTime,
                                     repartition_num=1000, cached=True)
    # df.show(10)
    # print('======count=======', df.count())

    result_rdd = df.rdd.mapPartitions(lambda x: send(x))
    result_df = sparkSession.createDataFrame(result_rdd)
    result_df.show(100, False)
    # 计算处理后的成功错误条数 比例
    result_df.registerTempTable('res_table')

    correct_num = sparkSession.sql("SELECT COUNT(*) from res_table WHERE status = 1").first()[0]
    wrong_num = sparkSession.sql("SELECT COUNT(*) from res_table WHERE status = 0").first()[0]

    print('======推送完成=======')
    print('成功数目：', correct_num, correct_num/(correct_num+wrong_num))
    print('失败数目：', wrong_num, wrong_num/(correct_num+wrong_num))

