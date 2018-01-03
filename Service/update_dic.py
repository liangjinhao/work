import pymysql.cursors
import pymysql
import configparser
import os
import inspect
from apscheduler.schedulers.blocking import BlockingScheduler
import threading
import logging
from logging.handlers import RotatingFileHandler

CONFIG_FILE = "path.conf"


class UpdateDict:

    """
    每天凌晨12：00定时从SQL数据库里拉取新的字典，更新到本地词典
    """

    def __init__(self):

        # 存放词典的SQL数据库相关配置
        self.host = '120.26.12.152'
        self.port = 3306
        self.db = 'search_word'
        self.table = 'dict_classification'
        self.user = 'team_bj'
        self.password = 'P9WdpTHVoX17eWPK'

        # 获取本地词典文件的路径
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)
        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        self.dict_path = home_dir + conf.get("dictionary", "phrase")

        self.connection = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                          db=self.db, charset='utf8', cursorclass=pymysql.cursors.DictCursor)

    def __del__(self):
        self.connection.close()

    def yield_data(self):
        """
        从SQL数据库里拉取新的字典
        :return:
        """

        sql = "SELECT * FROM " + self.db + "." + self.table + ";"
        cursor = self.connection.cursor()
        cursor.execute(sql)
        for row in cursor:
            word = row['dict_word']
            classification = row['classification']
            yield {'dict_word': word, 'classification': classification}

    def run(self):
        """
        更新本地词典文件
        :return:
        """

        # 设置日志
        handle = RotatingFileHandler('./dict.log', maxBytes=5 * 1024 * 1024, backupCount=1)
        handle.setLevel(logging.WARNING)
        log_formater = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        handle.setFormatter(log_formater)

        logger = logging.getLogger('Rotating log')
        logger.addHandler(handle)
        logger.setLevel(logging.WARNING)

        # SQL数据库里的分类与CRF标签的对应关系
        mappings = {'company_public_cn': 'subject1', 'company_public_hk': 'subject1',
                    'security_stock_shanghai': 'subject1', 'security_stock_shenzhen': 'subject1',
                    'security_stock_hongkong': 'subject1'}

        my_dict = {}
        with open(self.dict_path + '_local') as f:
            for line in f:
                if not line.startswith('#') and line != '\n':
                    my_dict[line.strip().split('\t')[0]] = line.strip().split('\t')[-1]

        unknown_classification = []
        for i in self.yield_data():
            if i['classification'] not in mappings and i['classification'] not in unknown_classification:
                unknown_classification.append(i['classification'])
            if i['classification'] in mappings:
                my_dict[i['dict_word']] = mappings[i['classification']]

        my_list = sorted(my_dict.items(), key=lambda x: x[0], reverse=True)
        with open(self.dict_path, 'w') as f:
            for i in my_list:
                f.write(i[0] + '\t' + i[-1] + '\n')

        for i in unknown_classification:
            logger.warning('Unkonwn classification: ' + i)


class UpdateDictThread(threading.Thread):

    """
    用于更新本地词典的线程
    """

    def run(self):
        f = UpdateDict()
        sched = BlockingScheduler()
        # 在每天凌晨 12：00 更新本地字典
        sched.add_job(f.run, 'cron', hour=16, minute=38)
        sched.start()
