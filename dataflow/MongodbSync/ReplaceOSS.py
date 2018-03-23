import pymongo
import logging
from logging.handlers import RotatingFileHandler
import threading


# MongoDB 信息
MONGODB_HOST = 'dds-j6cd3f25db6afa741.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'hk_sync'
PASSWORD = '9c9df8aebf04'


class ReplaceOSS(threading.Thread):

    def __init__(self):
        super(ReplaceOSS).__init__()

        # 记载 MongoDBListener 线程情况的 logger
        handle = RotatingFileHandler('./replace_oss.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        self.logger = logging.getLogger('ReplaceOSS')
        self.logger.addHandler(handle)
        # logger.setLevel(logging.INFO)

    def replace(self, table):

        count = 0
        client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        db = client['cr_data']
        db.authenticate(USER, PASSWORD)
        collection = db[table]

        cursor = collection.find().sort('$natural', pymongo.ASCENDING)
        self.logger.warning('开始替换' + table)
        for record in cursor:
            count += 1
            if count % 100000 == 0:
                if 'create_time' in record:
                    self.logger.warning(table + ' 已经替换 ' + str(count / 10000) + ' 万条数据')

            if table in ['hb_charts', 'hb_tables', 'juchao_charts', 'juchao_tables']:
                if 'pngFile' in record and 'oss-cn-hangzhou' in record['pngFile']:
                    new_pngFile = record['pngFile'].replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    collection.update_one({'_id': record['_id']}, {'$set': {'pngFile': new_pngFile}})
                    self.logger.info(table + ' 替换OSS: ' + record['_id'])
                if 'fileUrl' in record and record['fileUrl'] is not None:
                    new_fileUrl_oss = record['fileUrl'].replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    collection.update_one({'_id': record['_id']}, {'$set': {'fileUrl': new_fileUrl_oss}})
                    self.logger.info(table + ' 替换OSS: ' + record['_id'])

            elif table in ['hb_text', 'juchao_text']:
                if 'fileUrl' in record and 'oss-cn-hangzhou' in record['fileUrl']:
                    new_fileUrl_oss = record['fileUrl'].replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    collection.update_one({'_id': record['_id']}, {'$set': {'fileUrl': new_fileUrl_oss}})
                    self.logger.info(table + ' 替换OSS: ' + record['_id'])
                if 'html_file' in record and 'oss-cn-hangzhou' in record['html_file']:
                    new_html_file_oss = record['html_file'].replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    collection.update_one({'_id': record['_id']}, {'$set': {'html_file': new_html_file_oss}})
                    self.logger.info(table + ' 替换OSS: ' + record['_id'])
                if 'text_file' in record and 'oss-cn-hangzhou' in record['text_file']:
                    new_text_file_oss = record['text_file'].replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    collection.update_one({'_id': record['_id']}, {'$set': {'text_file': new_text_file_oss}})
                    self.logger.info(table + ' 替换OSS: ' + record['_id'])
                if 'paragraph_file' in record and 'oss-cn-hangzhou' in record['paragraph_file']:
                    new_paragraph_file_oss = record['paragraph_file'].replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                    collection.update_one({'_id': record['_id']}, {'$set': {'paragraph_file': new_paragraph_file_oss}})
                    self.logger.info(table + ' 替换OSS: ' + record['_id'])

        self.logger.warning(table + ' 已经替换玩所有的OSS')
        client.close()


if __name__ == '__main__':
    tables = ['hb_charts', 'hb_tables', 'hb_text',
              'juchao_charts', 'juchao_tables', 'juchao_text']
    for table in tables:
        ReplaceOSS().start()

