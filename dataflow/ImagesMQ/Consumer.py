import pika
import threading
import hashlib
import json
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.Hbase import *


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
        self.hbase_table = 'img_data'  # 写入的Hbase的表名

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
            row_key = bytes(hashlib.md5(item['img_oss'].encode()).hexdigest(), encoding='utf-8')

            mutations.append(Mutation(column=b'info:img_type', value=img_type))
            result.append(Hbase.BatchMutation(row=row_key, mutations=mutations))

        client.mutateRows(table_name, result, None)

        transport.close()

    def callback(self, ch, method, properties, body):
        """
        回调函数，处理从 RabbitMQ 队列里取回的数据
        :param ch:
        :param method:
        :param properties:
        :param body:
        :return: 从 RabbitMQ 队列里取回单条数据，格式为 '{"url": "http://bj-image.oss-cn-hangzhou-internal.aliyuncs.
        com/c95f3995d885f39e7fa0d63e60b491e9.jpg", "ok": true, "result": ['line_chart']}'
        """
        body = str(body, encoding='utf-8') if isinstance(body, bytes) else body
        body = json.loads(body)
        image_oss = str(body['url'])
        if body['ok']:
            image_type = str(body['result'])
        else:
            if body['error_msg'] != 'type':
                with open('img_error') as f:
                    f.write('rowKey: ' + str(bytes(hashlib.md5(image_oss.encode()).hexdigest(), encoding='utf-8')) +
                            ' img_oss: ' + image_oss + 'error_msg' + body['error_msg'])
            image_type = str([])
        data = {'img_oss': image_oss, 'img_type': image_type}

        self.write_hbase([data], self.hbase_table, self.thrift_ip, self.thrift_port)

        info = {'rowKey': bytes(hashlib.md5(image_oss.encode()).hexdigest(), encoding='utf-8'), 'img_oss':image_oss,
                'img_type': image_type}
        print(" Write %r" % str(info))

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
