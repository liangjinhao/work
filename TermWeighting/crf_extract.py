# -*- coding: utf-8 -*-
import os
import inspect
import copy
import ConfigParser
import CRFPP
from jpype import *

reload(sys)
sys.setdefaultencoding('utf-8')

CONFIG_FILE = "path.conf"


class CRFExtract:

    def __init__(self):
        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        conf = ConfigParser.ConfigParser()
        conf.read(CONFIG_FILE)
        class_path = conf.get("hanlp", "classpath")
        self.model_path = home_dir + conf.get("crf", "modelpath")
        startJVM(getDefaultJVMPath(), "-Djava.class.path=" + class_path, "-Xms1g", "-Xmx1g")  # 启动JVM
        self.HanLP = JClass('com.hankcs.hanlp.HanLP')

    def extract(self, string):
        """
        从一个句子中抽取出主体/指标/时间等信息
        :param sentence: 一个句子，字符串
        :param model: CRF++的模型文件的路径
        :param HanLP: HanLP的HanLP对象
        :return: 返回抽取的结果，是个字典
        """

        result_dict = {'subject': [], 'indicator': [], 'time': [], 'area': [], 'formula': [], 'frequency': [],
                       'conjunction': [], 'module': [], 'query': [], 'major': [], 'minor': [], 'description': [],
                       'definitive': [], 'useless': []}

        tag_list = {'1': 'subject', '2': 'indicator', '3': 'time', '4': 'area', '5': 'formula', '6': 'frequency',
                    '7': 'conjunction', '8': 'module', '9': 'query', 'a': 'major', 'b': 'minor', 'c': 'description',
                    'd': 'definitive', 'o': 'useless'}

        tagger = CRFPP.Tagger("-m " + self.model_path)
        res = self.HanLP.segment(string)
        word_list = []
        pos_list = []
        for item in res:
            word = item.word
            nature = java.lang.String.valueOf(item.nature)
            word_list.extend(list(word))
            pos_list.extend([nature] * len(list(word)))

        for i in range(len(word_list)):
            tagger.add((word_list[i] + '\t' + pos_list[i]).encode('utf-8'))

        tagger.parse()  # parse and change internal stated as 'parsed'
        result_dict_temp = copy.deepcopy(result_dict)
        previous_tag = ''
        size = tagger.size()  # token size
        xsize = tagger.xsize()  # column size
        for i in range(0, size):
            char = tagger.x(i, 0).decode('utf-8')
            tag = tagger.y2(i)
            tag_name = tag_list[tag]
            if previous_tag == '' or previous_tag != tag_name:
                result_dict_temp[tag_name].append(char)
            else:
                result_dict_temp[tag_name][-1] += char
            previous_tag = tag_name
        return result_dict_temp

    def get_hanlp_segment(self, sentence):
        """
        返回对sentence的分词结果list
        :param sentence: 
        :return: 
        """
        segment_result = []

        if sentence == '' or sentence is None:
            return segment_result

        sentence = sentence.decode('utf-8')
        res = self.HanLP.segment(sentence)
        for item in res:
            word = item.word
            segment_result.append(word)

        return segment_result

# a = CRFExtract()
# print a.extract(u'万科营业收入')