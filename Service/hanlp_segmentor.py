import configparser
from jpype import *
import re
from collections import Counter
import threading
import time
import logging

CONFIG_FILE = "path.conf"


class Singleton(type):

    _instances = dict()
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):

        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            with cls._lock:
                if cls in cls._instances:
                    cls._instances[cls].__init__(*args, **kwargs)

        return cls._instances[cls]


class HanlpSegmentor(metaclass=Singleton):

    def __init__(self):
        """
        启动JVM生产Hanlp实例
        """
        start_ts = time.time()
        logging.info('Hanlp 开始启动')

        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)
        class_path = conf.get("hanlp", "classpath")

        if not isJVMStarted():
            startJVM(getDefaultJVMPath(), "-Djava.class.path=" + class_path, "-Xms3g", "-Xmx3g")  # 启动JVM
        self.NLPTokenizer = JClass('com.hankcs.hanlp.tokenizer.NLPTokenizer')
        self.NLPTokenizer.SEGMENT.enableIndexMode(False)  # 把Index模式关闭
        self.CustomDictionry = JClass('com.hankcs.hanlp.dictionary.CustomDictionary')

        logging.info('Hanlp 启动完成 ' + str((time.time() - start_ts)) + "s")

    def reload_custom_dictionry(self):
        """
        重新加载自定义字典
        :return:
        """
        self.CustomDictionry.reload()
        logging.info('完成 Reload Hanlp 自定义词典')

    def get_segments(self, sentence):
        """
        对一个句子进行分词
        :param sentence: 输入的句子
        :return: 分词的结果（词和词性组成的元组的列表）
        """
        res = list(self.NLPTokenizer.segment(sentence))
        result = []
        for item in res:
            word = item.word
            nature = java.lang.String.valueOf(item.nature)
            result.append((word, nature))
        result = self.quotation_split(result)
        return result

    def quotation_split(self, segments_result):
        """
        处理最初的分词结果，被引号（即“,”,","）包括的词将不被切开，形成新的分词结果
        :param segments_result:
        :return:
        """
        sentence = ''.join([i[0] for i in segments_result])

        quotation_range = [(i.start(), i.end()) for i in re.finditer('(?:"[\s\S]+?")|(?:“[\s\S]+?”)', sentence)]
        previous_quotation = []
        offset = 0
        result = []
        for i in segments_result:
            ran = self.in_which_range((offset, offset+len(i[0])), quotation_range)
            if ran == ():
                result.append(i)
            else:
                if offset+len(i[0]) < ran[1]:
                    previous_quotation.append(i)
                else:
                    previous_quotation.append(i)
                    word = ''.join([x[0] for x in previous_quotation])
                    word = '"' + word[1:-1] + '"'
                    temp = []
                    [temp.extend([x[1]] * len(x[0])) for x in previous_quotation]
                    nature = Counter(temp).most_common(1)[0][0]
                    result.append((word, nature))
                    previous_quotation = list()
            offset += len(i[0])
        return result

    @staticmethod
    def in_which_range(range_one, range_list):
        """
        判断一个区间在几个区间内的哪一个，比如，(2,3) 处于[(1,3),(5,7)]中的(1,3)，但在[(1,4)]中则没有，即()
        :param range_one:
        :param range_list:
        :return:
        """
        for i in range_list:
            if i[0] <= range_one[0] and range_one[1] <= i[1]:
                return i
        return ()
