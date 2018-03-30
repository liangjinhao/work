import pymongo
import logging
from logging.handlers import RotatingFileHandler
import threading
import traceback
import time
import os
import errno


"""
这个脚本主要用于将香港 Mongo 集群的 'hb_charts', 'hb_tables', 'hb_text', 'juchao_charts', 'juchao_tables', 'juchao_text' 这6个
表中的某些字段里的含有指向国内 Mongo 集群的 OSS 链接替换成指向国外 Mongo 集群的 OSS 链接

此脚本需要遍历整个 MongoDB 表，可能会耗时很长

此问题的来源原因是：
1. 公司最开始在国内的阿里云上搭建了 Mongo 集群，这些集群里的大的文件对象存储在国内阿里云的 OSS 服务中，而 MongoDB 的表里则存着指向阿里云 OSS 的链接
2. 后来公司业务需要，又在香港的阿里云上搭建了海外 Mongo 集群和海外的阿里云的 OSS 服务，然后把国内的数据不作修改直接克隆到了国外的 Mongo 集群和 OSS 服务
3. 单直接克隆过去的 MongoDB 的某些字段还指向的是国内OSS，所以需要替换 OSS 连接重新指向香港的 OSS 服务，此脚本只替换了上面提及的 6 个表
"""


# 香港 MongoDB 集群连接信息
MONGODB_HOST = 'dds-j6cd3f25db6afa741.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'hk_sync'
PASSWORD = '9c9df8aebf04'


class ReplaceOSS(threading.Thread):

    def __init__(self, table):
        super(ReplaceOSS, self).__init__()

        self.log_dir = './log'
        try:
            os.makedirs(self.log_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        self.table = table

    def run(self):

        count = 0
        table = self.table

        handle = RotatingFileHandler(self.log_dir + '/' + table + '.log', maxBytes=50 * 1024 * 1024, backupCount=3)
        handle.setFormatter(logging.Formatter(
            '%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'))

        logger = logging.getLogger(table)
        logger.addHandler(handle)
        # logger.setLevel(logging.INFO)

        client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT, unicode_decode_error_handler='ignore')
        db = client['cr_data']
        db.authenticate(USER, PASSWORD)
        collection = db[table]

        cursor = collection.find().sort('$natural', pymongo.ASCENDING)
        logger.warning('开始替换' + table)
        for record in cursor:

            try:
                # 这三个表的 pngFile 和 fileUrl 字段有 oss 链接
                if table in ['hb_charts', 'hb_tables', 'juchao_charts', 'juchao_tables']:
                    if 'pngFile' in record and record['pngFile'] is not None:
                        new_pngFile = record['pngFile'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                            .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                        collection.update_one({'_id': record['_id']}, {'$set': {'pngFile': new_pngFile}})
                        # logger.info(table + ' 替换OSS: ' + str(record['_id']))
                    if 'fileUrl' in record and record['fileUrl'] is not None:
                        new_fileUrl_oss = record['fileUrl'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                            .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                        collection.update_one({'_id': record['_id']}, {'$set': {'fileUrl': new_fileUrl_oss}})
                        # logger.info(table + ' 替换OSS: ' + str(record['_id']))
                    count += 1
                # 这三个表的 fileUrl，html_file，text_file 和 paragraph_file 字段有 oss 链接
                elif table in ['hb_text', 'juchao_text']:
                    if 'fileUrl' in record and record['fileUrl'] is not None:
                        new_fileUrl_oss = record['fileUrl'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                            .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                        collection.update_one({'_id': record['_id']}, {'$set': {'fileUrl': new_fileUrl_oss}})
                        # logger.info(table + ' 替换OSS: ' + str(record['_id']))
                    if 'html_file' in record and record['html_file'] is not None:
                        new_html_file_oss = record['html_file'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                            .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                        collection.update_one({'_id': record['_id']}, {'$set': {'html_file': new_html_file_oss}})
                        # logger.info(table + ' 替换OSS: ' + str(record['_id']))
                    if 'text_file' in record and record['text_file'] is not None:
                        new_text_file_oss = record['text_file'].replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                            .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                        collection.update_one({'_id': record['_id']}, {'$set': {'text_file': new_text_file_oss}})
                        # logger.info(table + ' 替换OSS: ' + str(record['_id']))
                    if 'paragraph_file' in record and record['paragraph_file'] is not None:
                        new_paragraph_file_oss = record['paragraph_file']\
                            .replace('abc-crawler.oss-cn', 'hk-crawler.oss-cn')\
                            .replace('oss-cn-hangzhou', 'oss-cn-hongkong')
                        collection.update_one({'_id': record['_id']}, {'$set': {'paragraph_file': new_paragraph_file_oss}})
                        # logger.info(table + ' 替换OSS: ' + str(record['_id']))
                    count += 1

                if count % 100000 == 0:
                    if 'create_time' in record:
                        logger.warning(table + ' 已经替换 ' + str(count / 10000) + ' 万条数据')
            except:
                logger.error(traceback.format_exc())
                time.sleep(0.5)

            time.sleep(0.001)

        logger.warning(table + ' 已经替换完所有的OSS')
        client.close()


if __name__ == '__main__':
    tables = ['hb_charts', 'hb_tables', 'hb_text',
              'juchao_charts', 'juchao_tables', 'juchao_text']
    for table_name in tables:
        t = ReplaceOSS(table_name)
        t.start()

