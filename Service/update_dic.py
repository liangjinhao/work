import pymysql.cursors
import pymysql
import configparser
import os
import inspect
from apscheduler.schedulers.blocking import BlockingScheduler
import threading
import logging
from logging.handlers import RotatingFileHandler
import time

CONFIG_FILE = "path.conf"


class UpdateDict:

    """
    每天凌晨12：00定时从SQL数据库里拉取新的字典，更新到本地词典
    """

    def __init__(self):

        # 存放词典的SQL数据库相关配置
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)

        self.host = str(conf.get("sql_dict", "host"))
        self.port = int(conf.get("sql_dict", "port"))
        self.db = conf.get("sql_dict", "db")
        self.table = conf.get("sql_dict", "table")
        self.user = conf.get("sql_dict", "user")
        self.password = conf.get("sql_dict", "password")

        # 获取本地词典文件的路径
        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        self.hanlp_path = conf.get('hanlp', 'classpath').split(':')[-1]
        self.dict_path = home_dir + conf.get("dictionary", "phrase")

        self.connection = pymysql.connect(host=self.host, port=self.port, db=self.db,
                                          user=self.user, password=self.password, charset='utf8',
                                          cursorclass=pymysql.cursors.DictCursor)

    def __del__(self):
        self.connection.close()

    def yield_data(self):
        """
        从SQL数据库里拉取新的字典
        :return:
        """

        sql = "SELECT dict_word, classification FROM " + self.db + "." + self.table + ";"
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
        handle.setFormatter(
            logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
        )

        logger = logging.getLogger('MySqlLogger')
        logger.addHandler(handle)
        logger.setLevel(logging.INFO)

        print(time.strftime('%Y-%m-%d %H:%M:%S'), '开始从线上 MySql字典库 拉取更新词典')
        logger.info('开始从线上 MySql字典库 拉取更新词典')

        hanlp_dict_set = set()

        with open(self.hanlp_path + 'data/dictionary/custom/abcChinese.txt') as abcC:
            for line in abcC:
                hanlp_dict_set.add(line.split(' ')[0])

        with open(self.hanlp_path + 'data/dictionary/custom/abcEnglish.eng') as abcE:
            for line in abcE:
                hanlp_dict_set.add(line.split('##')[0])

        my_dict = {}
        with open(self.dict_path) as f:
            for line in f:
                if not line.startswith('#') and line != '\n':
                    my_dict[line.strip().split('\t')[0]] = line.strip().split('\t')[-1]

        for i in self.yield_data():
            if i['dict_word'] in hanlp_dict_set or len(i['dict_word']) > 4:
                continue
            if '指标' in i['classification']:
                my_dict[i['dict_word']] = 'indicator'
            else:
                my_dict[i['dict_word']] = 'subject1'

        my_list = sorted(my_dict.items(), key=lambda x: x[0], reverse=True)
        print(time.strftime('%Y-%m-%d %H:%M:%S'), '本次生成词语 ' + str(len(my_list)) + ' 个')
        logger.info('本次生成词语 ' + str(len(my_list)) + ' 个')
        with open(self.dict_path + '_local', 'w') as f:
            for i in my_list:
                f.write(i[0] + '\t' + i[-1] + '\n')


class UpdateDictThread(threading.Thread):

    """
    用于更新本地词典的线程
    """

    def run(self):
        f = UpdateDict()
        sched = BlockingScheduler()
        # 在每天凌晨 12：00 更新本地字典
        sched.add_job(f.run, 'cron', hour=20, minute=47)
        sched.start()
