import pika
import threading
import hashlib
import json
import redis
import requests
import datetime
import time
import Utils
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.Hbase import *
import logging
from logging.handlers import RotatingFileHandler

handle = RotatingFileHandler('./NewImagePushing.log', maxBytes=5 * 1024 * 1024, backupCount=1)
handle.setLevel(logging.INFO)
log_formater = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
handle.setFormatter(log_formater)

logger = logging.getLogger('Rotating log')
logger.addHandler(handle)
logger.setLevel(logging.INFO)


class ScrawlImagesConsumer(threading.Thread):
    """
    消费线程，从图像处理的RabbitMQ队列里取出数据写入到Hbase中
    """

    def __init__(self):
        super(ScrawlImagesConsumer, self).__init__()

        self.vhost = 'search'  # RabbitMQ 虚拟主机
        self.username = 'search'  # RabbitMQ 用户名
        self.password = '0dx8iYF3rII91YRe'  # RabbitMQ 密码
        self.RabbitMQ_ip = '47.98.34.75'  # RabbitMQ IP地址
        self.RabbitMQ_port = 5672  # RabbitMQ 端口
        self.queue_name = 'labeled_scrawl_images'  # RabbitMQ 取数据的队列名字

        self.thrift_ip = '10.27.71.108'  # thrift IP地址
        self.thrift_port = 9099  # thrift 端口
        self.hbase_table = b'news_img_data'  # 写入的Hbase的表名

        self.redis_ip = '10.174.97.43'
        self.redis_port = 6379
        self.redis_queue = 'oss_img_tag_queue'

        self.post_url = 'http://10.24.235.15:8080/solrweb/chartIndexByUpdate'

    @staticmethod
    def write_hbase(data, table_name, ip, server_port):
        """
        将数据写入Hbase中
        :param data: 包含数据的迭代器，单条数据为dict类型，比如 {'img_oss' = 'http://bj-image.oss-cn-hangzhou-internal.
        aliyuncs.com/6321965c0c96f1ea809b15ad757252f3.jpeg', 'img_type' = ['line_chart']}
        :param table_name: 需要推送的目标表的表名
        :param ip: 推送的目标thrift ip
        :param server_port: 推送的目标thrift port
        """

        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')

        # 建立 thrift 连接
        transport = TSocket.TSocket(ip, server_port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()

        result = []
        count = 0
        for item in data:
            count += 1
            mutations = []
            img_type = bytes(item['img_type'], encoding='utf-8')
            row_key = bytes(hashlib.md5(item['url'].encode()).hexdigest(), encoding='utf-8')

            mutations.append(Mutation(column=b'info:img_type', value=img_type))
            result.append(Hbase.BatchMutation(row=row_key, mutations=mutations))

        client.mutateRows(table_name, result, None)

        transport.close()

    def get_hbase_row(self, rowkey):
        """
        通过 Thrift读取 Hbase 中 键值为 rowkey 的某一行数据
        :param rowkey:
        :return:
        """
        rowkey = bytes(rowkey, encoding='utf-8') if isinstance(rowkey, str) else rowkey
        transport = TSocket.TSocket(self.thrift_ip, self.thrift_port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        row = client.getRow(self.hbase_table, rowkey, attributes=None)
        if len(row) > 0:
            result = dict()
            result['rowKey'] = str(row[0].row, 'utf-8')
            columns = row[0].columns
            for column in columns:
                result[str(column, 'utf-8').split(':')[-1]] = str(columns[column].value, 'utf-8')
            return result
        else:
            return {}

    def send(self, row):
        """
        将 Hbase 中的一行数据归一化后推送到 Solr 中
        """
        if row == {} or row is None:
            return

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

        row_key = row['rowKey']  #
        img_oss = row['img_oss']  # 图片 OSS 链接
        img_title = row['img_title'] if 'img_title' in row else ''
        img_type = row['img_type'] if 'img_type' in row else '[]'
        title = row['title'] if 'title' in row else ''
        url = row['url'] if 'url' in row else ''  # 图片所在文章的链接

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

        if row['publish_time'] is not None and Utils.time_norm(row['publish_time']) != '':
            img_datetime = datetime.datetime.strptime(Utils.time_norm(row['publish_time']), '%Y-%m-%d %H:%M:%S')
            img_json['time'] = int(time.mktime(img_datetime.timetuple()))
            img_json['year'] = img_datetime.year
        else:
            logger.warning('数据' + row['url'] + '的时间 [' + str(row['publish_time']) + '] 解析失败')

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

        if img_json['image_title'] != '':
            requests.post(self.post_url, json=[img_json])

        requests.post(self.post_url, json=[img_json])

        try:
            response = requests.post(self.post_url, json=[img_json])
            if response.status_code != 200:
                logger.error("推送 Solr 返回响应代码 " + str(response.status_code) + "，数据 rowKey:" + row['rowKey'])
                redis_client = redis.Redis(host=self.redis_ip, port=self.redis_port)
                put_data = {'url': row['img_url'], 'oss_url': row['img_oss']}
                redis_client.rpush(self.redis_queue, str(put_data))
            else:
                logger.info("推送 Solr 完成， rowKey:" + row['rowKey'])
        except Exception as e:
            logger.exception("推送 Solr 异常： " + str(e) + "，数据 rowKey:" + row['rowKey'])
            redis_client = redis.Redis(host=self.redis_ip, port=self.redis_port)
            put_data = {'url': row['img_url'], 'oss_url': row['img_oss']}
            redis_client.rpush(self.redis_queue, str(put_data))

    def callback(self, ch, method, properties, body):
        """
        回调函数，处理从 RabbitMQ 队列里取回的数据
        :param ch:
        :param method:
        :param properties:
        :param body: 从 RabbitMQ 队列里取回单条数据，格式为
        '{"url": "http://somesite/c95f3995d885f39e7fa0d63e60b491e9.jpg",
        "oss_url": "http://bj-image.oss-cn-hangzhou-internal.aliyuncs.com/c95f3995d885f39e7fa0d63e60b491e9.jpg",
        "ok": true,
        "result": ['line_chart']}'
        :return:
        """
        body = str(body, encoding='utf-8') if isinstance(body, bytes) else body
        body = json.loads(body)
        url = hashlib.md5(str(body['url']).encode()).hexdigest()
        if body['ok']:
            image_type = str(body['result'])
        else:
            image_type = str([])
        data = {'url': url, 'img_type': image_type}

        try:
            self.write_hbase([data], self.hbase_table, self.thrift_ip, self.thrift_port)
        except Exception:
            logger.exception('写入 Hbase 失败， Url:' + url)

        img = self.get_hbase_row(url)
        self.send(img)

        # To process body
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):

        credentials = pika.PlainCredentials(self.username, self.password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.RabbitMQ_ip, port=self.RabbitMQ_port,
                                                                       virtual_host=self.vhost,
                                                                       credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self.queue_name,
            }
        )

        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(self.callback, queue=self.queue_name)
        channel.start_consuming()
