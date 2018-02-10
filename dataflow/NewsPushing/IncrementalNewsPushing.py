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


"""
该脚本读取Redis队列里的每日增量的新闻数据（Redis队列里存储的是Hbase的新闻的rowkey），然后通过thrift访问Hbase，按照rowkey取出新闻的数据
并作相关字段转换后post到Solr服务上
"""


REDIS_IP = ''
REDIS_PORT = 6379
REDIS_QUEUE = ''

THRIFT_IP = '10.27.71.108'
THRIFT_PORT = 9099
HBASE_TABLE_NAME = 'news_data'

POST_URL = 'http://10.168.20.246:8080/solrweb/indexByUpdate?single=true&core_name=core_news'


def get_hbase_row(rowkey):
    """
    通过 Thrift读取 Hbase 中 键值为 rowkey 的某一行数据
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
        columns = row[0].columns
        for column in columns:
            result[str(column, 'utf-8').split(':')[-1]] = str(columns[column].value, 'utf-8')
        return result
    else:
        return {}


def post(rowkey, news_json, write_back_redis=False):
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
        if response.status_code != '200' and write_back_redis:
            print(rowkey, response.status_code)
            redis_client.rpush(REDIS_QUEUE, rowkey)
    except Exception as e:
        print(e)
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

            normed_publish_time = Utils.time_norm(row['publish_time'])
            news_json['publish_time'] = normed_publish_time if normed_publish_time != '' else row['crawl_time']

            news_json['source_name'] = row['source']
            news_json['title'] = row['title']
            news_json['url'] = row['url']

            if news_json['publish_time'] is not None:
                news_json['time'] = int(datetime.datetime.strptime(news_json['publish_time'], '%Y-%m-%d %H:%M:%S')
                                        .strftime('%s'))
            else:
                news_json['time'] = 0

            executor.submit(post, row['id'], news_json)


if __name__ == '__main__':

    count = 0
    news_list = []
    while True:
        redis_ip = ''
        redis_port = 6379
        redis_queue_name = ''
        r = redis.Redis(host=redis_ip, port=redis_port)
        rowkey = r.lpop(name=redis_queue_name)
        if not rowkey:
            print('Redis 队列中无数据，等待5分钟再取')
            time.sleep(5 * 60)
            rowkey = str(rowkey, encoding='utf-8') if isinstance(rowkey, bytes) else rowkey
        else:
            news = get_hbase_row(rowkey)
            if count < 100:
                news_list.append(news)
            else:
                send(news_list)
                count = 0
                news_list = []
