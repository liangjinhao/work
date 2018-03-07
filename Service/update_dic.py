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
        self.host = '10.140.85.247'
        self.port = 6633
        self.db = 'search_word'
        self.table = 'dict_classification'
        self.user = 'team_dict_ro'
        self.password = '8f7c6535b895'

        # 获取本地词典文件的路径
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)
        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        self.hanlp_path = conf.get('hanlp', 'classpath').split(':')[-1]
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

        logger.info('开始从线上 MySql字典库 拉取更新词典')

        hanlp_dict_set = set()

        with open(self.hanlp_path + 'data/dictionary/custom/abcChinese.txt') as abcC:
            for line in abcC:
                hanlp_dict_set.add(line.split(' ')[0])

        with open(self.hanlp_path + 'data/dictionary/custom/abcEnglish.eng') as abcE:
            for line in abcE:
                hanlp_dict_set.add(line.split('##')[0])

        my_dict = {}
        with open(self.dict_path + '_local') as f:
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
        logger.info('本次生成词语 ' + str(len(my_list)) + ' 个')
        with open(self.dict_path, 'w') as f:
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
        sched.add_job(f.run, 'cron', hour=0, minute=0)
        sched.start()
