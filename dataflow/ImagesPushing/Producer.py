import pika
import time
import redis
import threading
import logging
from logging.handlers import RotatingFileHandler


# 记载 Producer 线程情况的 logger
producer_handle = RotatingFileHandler('./producer.log', maxBytes=5 * 1024 * 1024, backupCount=1)
producer_handle.setFormatter(
    logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
)
logger_producer = logging.getLogger('producer log')
logger_producer.addHandler(producer_handle)
logger_producer.setLevel(logging.INFO)


class ScrawlImagesProducer(threading.Thread):
    """
    生产线程，从redis里取出图片送至图像处理的RabbitMQ里去
    """

    def __init__(self):
        super(ScrawlImagesProducer, self).__init__()

        self.vhost = 'search'  # RabbitMQ 虚拟主机
        self.username = 'search'  # RabbitMQ 用户名
        self.password = '0dx8iYF3rII91YRe'  # RabbitMQ 密码
        self.RabbitMQ_ip = '47.98.34.75'  # RabbitMQ IP地址
        self.RabbitMQ_port = 5672  # RabbitMQ 端口
        self.queue_name = 'scrawl_images'  # RabbitMQ 送入的队列名字

        self.redis_ip = '10.174.97.43'  # Redis IP地址
        self.redis_port = 6379  # Redis 端口
        self.redis_queue_name = 'oss_img_tag_queue'  # Redis 取出数据的队列

    def run(self):

        credentials = pika.PlainCredentials(self.username, self.password)
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.RabbitMQ_ip, port=self.RabbitMQ_port,
                                      virtual_host=self.vhost, credentials=credentials))
        channel = connection.channel()
        channel.queue_declare(
            queue=self.queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "",
                "x-dead-letter-routing-key": self.queue_name,
            }
        )

        r = redis.Redis(host=self.redis_ip, port=self.redis_port)

        while True:
            message = r.lpop(name=self.redis_queue_name)
            if not message:
                logger_producer.info("Redis 队列中无数据，等待5s再取")
                connection.close()
                time.sleep(5)
                continue
            message = str(message, encoding='utf-8') if isinstance(message, bytes) else message
            result = channel.basic_publish(exchange='',
                                           routing_key=self.queue_name,
                                           body=message,
                                           properties=pika.BasicProperties(
                                               delivery_mode=2,  # make message persistent
                                           ))
            if result:
                logger_producer.info("从 Redis 推送数据到 RabbitMQ 成功： " + message)
            else:
                logger_producer.error("从 Redis 推送数据到 RabbitMQ 失败： " + message)
                r.rpush(self.redis_queue_name, message)

        # connection.close()
