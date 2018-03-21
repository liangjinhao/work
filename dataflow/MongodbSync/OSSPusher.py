import redis
import threading
import time
import oss2
import logging
from logging.handlers import RotatingFileHandler
import traceback

# Redis 信息
REDIS_IP = '47.97.27.84'
REDIS_PORT = 6379
OSS_QUEUE = 'oss'

# OSS 信息
ACCESS_KEY_ID = 'LTAIUckqp8PIWkm9'
ACCESS_KEY_SECRET = 'jCsTUNa9l9zloXzdW6xvksFpDaMwY1'
BUCKET_HZ = 'abc-crawler'
BUCKET_HK = 'hk-crawler'
ENDPOINT_HZ = 'oss-cn-hangzhou.aliyuncs.com'
ENDPOINT_HK = 'oss-cn-hongkong.aliyuncs.com'


class OSSPusher(threading.Thread):

    def __init__(self):
        super(OSSPusher, self).__init__()

        # 记载 OSSPusher 线程情况的 logger
        handle = RotatingFileHandler('./oss_pusher.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger(__name__)
        self.logger.addHandler(handle)
        # self.logger.setLevel(logging.INFO)

        self.bucket_hz = oss2.Bucket(oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET), ENDPOINT_HZ, BUCKET_HZ)
        self.bucket_hk = oss2.Bucket(oss2.Auth(ACCESS_KEY_ID, ACCESS_KEY_SECRET), ENDPOINT_HK, BUCKET_HK)

    def run(self):
        r = redis.Redis(host=REDIS_IP, port=REDIS_PORT)

        while True:

            oss_data = r.lpop(name=OSS_QUEUE)

            if oss_data:
                oss_data = oss_data if isinstance(oss_data, str) else str(oss_data, encoding='utf-8')
                oss_new = oss_data.replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                file_name = oss_data.split('aliyuncs.com/')[-1]
                try:
                    if not self.bucket_hk.object_exists(file_name) and self.bucket_hz.object_exists(file_name):
                        # print('开始下载', oss_data)
                        file_stream = self.bucket_hz.get_object(file_name)
                        # print('开始上传', oss_new)
                        self.bucket_hk.put_object(file_name, file_stream)
                        self.logger.info(str(r.llen(OSS_QUEUE)) + '    转写 oss 成功，oss 为: ' + oss_new)
                except Exception as e:
                    self.logger.error(str(r.llen(OSS_QUEUE)) + '    转写 oss 失败，oss 为: ' + oss_new + '错误为: \n'
                                      + traceback.format_exc())
                    r.rpush(OSS_QUEUE, oss_data)
                    time.sleep(0.001)
            else:
                self.logger.info('Redis oss 队列中无数据，等待10s再取')
                time.sleep(10)
