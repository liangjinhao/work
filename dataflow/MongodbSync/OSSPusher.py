import redis
import threading
import time
import oss2
import logging
from logging.handlers import RotatingFileHandler


# Redis 信息
REDIS_IP = '47.97.27.84'
REDIS_PORT = 6379
OSS_QUEUE = 'oss'

# OSS 信息
ACCESS_KEY_ID = 'LTAIUckqp8PIWkm9'
ACCESS_KEY_SECRET = 'jCsTUNa9l9zloXzdW6xvksFpDaMwY1'
BUCKET_HZ = 'abc-crawler'
BUCKET_HK = 'hk-crawler'
ENDPOINT_HZ = 'oss-cn-hangzhou-internal.aliyuncs.com'
ENDPOINT_HK = 'oss-cn-hongkong-internal.aliyuncs.com'


class OSSPusher(threading.Thread):

    def __init__(self):
        super(OSSPusher, self).__init__()

        # 记载 OSSPusher 线程情况的 logger
        handle = RotatingFileHandler('./OssPusher.log', maxBytes=5 * 1024 * 1024, backupCount=5)
        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handle)

        self.bucket_hz = oss2.Bucket(oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET), ENDPOINT_HZ, BUCKET_HZ)
        self.bucket_hk = oss2.Bucket(oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET), ENDPOINT_HK, BUCKET_HK)

    def run(self):
        while True:
            r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
            oss_data = r.lpop(name=OSS_QUEUE)
            oss_data = oss_data if isinstance(oss_data, str) else str(oss_data, encoding='utf-8')

            if oss_data:
                oss_new = oss_data.replace('hangzhou.aliyuncs', 'hongkong.aliyuncs')
                try:
                    if not self.bucket_hk.object_exists(oss_new):
                        file = self.bucket_hz.get_object(oss_data)
                        self.bucket_hk.put_object(oss_new, file)
                        self.logger.info('转写 oss 成功，oss 为: ' + oss_new)
                except Exception as e:
                    self.logger.error('转写 oss 失败，oss 为: ' + oss_new + '错误为: ' + str(e))
                    r.rpush(OSS_QUEUE, oss_data)
            else:
                # self.logger.info('Redis oss 队列中无数据，等待1s再取')
                time.sleep(1)
