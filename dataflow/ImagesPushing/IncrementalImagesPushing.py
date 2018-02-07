import hashlib
import datetime
import time
import redis
import requests
import concurrent.futures
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.Hbase import *


"""
该脚本读取Redis队列里的每日增量的图片数据（Redis队列里存储的是Hbase的图片的rowkey），然后通过thrift访问Hbase，按照rowkey取出图片的数据
并作相关字段转换后post到Solr服务上
"""


REDIS_IP = ''
REDIS_PORT = 6379
REDIS_QUEUE = ''

THRIFT_IP = '10.27.71.108'
THRIFT_PORT = 9099
HBASE_TABLE_NAME = b'news_data'

POST_URL = 'http://10.24.235.15:8080/solrweb/chartIndexByUpdate'


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


def send(x):
    """
    将数据作对应字段转换后 post 到 Solr 服务
    :param x:
    :return:
    """

    post_imgs = []
    post_count = 0

    for row in x:

        if row is {}:
            continue

        # Solr 里的图片的字段，将 Hbase 中存储的图片信息转换到对应字段上
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

        img_datetime = datetime.datetime.strptime(row['publish_time'], '%Y-%m-%d  %H:%M:%S')
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

    count = 0
    news_list = []
    while True:
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
        rowkey = r.lpop(name=REDIS_QUEUE)
        if not rowkey:
            send(news_list)
            count = 0
            news_list = []
            print('Redis 队列中无数据，等待5分钟再取')
            time.sleep(5 * 60)
        else:
            rowkey = str(rowkey, encoding='utf-8') if isinstance(rowkey, bytes) else rowkey
            news = get_hbase_row(rowkey)
            if count < 1000:
                news_list.append(news)
            else:
                send(news_list)
                count = 0
                news_list = []
