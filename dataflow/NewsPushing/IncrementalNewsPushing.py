import datetime
import Utils
import time
import redis
import requests
import concurrent.futures
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.Hbase import *
from logging.handlers import RotatingFileHandler


"""
该脚本读取Redis队列里的每日增量的新闻数据（Redis队列里存储的是Hbase的新闻的rowkey），然后通过thrift访问Hbase，按照rowkey取出新闻的数据
并作相关字段转换后post到Solr服务上
"""


REDIS_IP = '10.174.97.43'
REDIS_PORT = 6379
REDIS_QUEUE = 'index_pending_queue'

THRIFT_IP = '10.27.71.108'
THRIFT_PORT = 9099
HBASE_TABLE_NAME = b'news_data'

POST_URL = 'http://10.168.20.246:8080/solrweb/indexByUpdate?single=true&core_name=core_news'


def get_hbase_row(rowkey):
    """
    通过 Thrift读取 Hbase 中 键值为 rowkey 的某一行数据，返回该行数据的 dict 表示
    :param rowkey:
    :return:
    """
    rowkey = bytes(rowkey, encoding='utf-8') if isinstance(rowkey, str) else rowkey
    transport = TSocket.TSocket(THRIFT_IP, THRIFT_PORT)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Hbase.Client(protocol)
    transport.open()
    row = client.getRow(HBASE_TABLE_NAME, rowkey, attributes=None)
    if len(row) > 0:
        result = dict()
        result['rowKey'] = str(row[0].row, 'utf-8')
        columns = row[0].columns
        for column in columns:
            result[str(column, 'utf-8').split(':')[-1]] = str(columns[column].value, 'utf-8')
        return result
    else:
        logger.error("未在 Hbase 中找到该条数据，请求rowKey为:" + str(rowkey, encoding='utf-8'))
        return {}


def post(rowkey, news_json, write_back_redis=True):
    """
    将单条数据 post 到 Solr 服务上
    :param rowkey:
    :param news_json:
    :param write_back_redis: 是否将 post 失败的数据再重新写回到 Redis
    :return:
    """
    redis_client = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
    try:
        response = requests.post(POST_URL, json=[news_json])
        if response.status_code != 200 and write_back_redis:
            logger.error("推送 Solr 返回响应代码 " + str(response.status_code) + "，数据 rowKey:" + rowkey)
            redis_client.rpush(REDIS_QUEUE, rowkey)
        else:
            logger.info("推送 Solr 完成， rowKey:" + rowkey)
    except Exception as e:
        logger.error("推送 Solr 异常： " + str(e) + "，数据 rowKey:" + rowkey)
        if write_back_redis:
            redis_client.rpush(REDIS_QUEUE, rowkey)


def send(x):
    """
    将数据作对应字段转换后 post 到 Solr 服务
    :param x:
    :return:
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:

        for row in x:
            if row is None or not isinstance(row, dict) or row == {}:
                return
            news_json = dict({
                "id": "",
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
                "tags": "",
                'doc_score': 1.0,
                "time": 0,
            })

            news_json['id'] = row['rowKey']
            news_json['author'] = row['author']

            news_json['author'] = Utils.author_norm(row['author']) \
                if row['author'] is not None else row['author']

            news_json['category'] = row['category']
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
            news_json['tags'] = row['tag']

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

            executor.submit(post, row['rowKey'], news_json)


if __name__ == '__main__':

    # 记载消息推送的 logger
    handle = RotatingFileHandler('./news_pushing.log', maxBytes=5 * 1024 * 1024, backupCount=1)
    handle.setLevel(logging.INFO)
    handle.setFormatter(
        logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    )
    logger = logging.getLogger('NewsPushing')
    logger.addHandler(handle)
    logger.setLevel(logging.INFO)

    # 记载时间解析失败例子的 logger
    time_parsing_handle = RotatingFileHandler('./time_parsing.log', maxBytes=10 * 1024 * 1024, backupCount=3)
    time_parsing_handle.setFormatter(
        logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
    )
    logger_time_parsing = logging.getLogger('TimeParsingLogger')
    logger_time_parsing.addHandler(time_parsing_handle)
    logger_time_parsing.setLevel(logging.INFO)

    count = 0
    news_list = []
    while True:
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
        rowkey = r.lpop(name=REDIS_QUEUE)

        if rowkey:
            rowkey = str(rowkey, encoding='utf-8') if isinstance(rowkey, bytes) else rowkey
            news = get_hbase_row(rowkey)
            send([news])
        else:
            logger.info('Redis 队列中无数据，等待5s再取')
            time.sleep(5)
