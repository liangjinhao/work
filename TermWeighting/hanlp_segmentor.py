# -*- coding: utf-8 -*-
from jpype import *
import inspect
import os
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('..')


class HanLPSegmentor:

    def __init__(self):
        self.class_path = '/home/yancheng/Code/crf_service/hanlp/hanlp-1.3.4.jar:/home/yancheng/Code/crf_service/hanlp/'
        if not isJVMStarted():
            startJVM(getDefaultJVMPath(), "-Djava.class.path=" + self.class_path, "-Xms1g", "-Xmx1g")  # 启动JVM
        self.HanLP = JClass('com.hankcs.hanlp.HanLP')

    def load_hanlp_config(self, config_file):
        """
        加载配置文件
        class_path=/niub/hanlp/hanlp-1.3.4.jar:/niub/hanlp/
        :param config_file: 
        :return: 
        """

        result = {'class_path': ''}
        with open(config_file, 'r') as f:
            for line in f:
                line = line.strip().strip('\n')
                if line.startswith('#') or line == '':
                    continue
                pair = line.split('=')
                if pair[0] == 'class_path':
                    result[pair[0]] = pair[1]
                    break
        return result

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

# a = HanLPSegmentor()
# print a.get_hanlp_segment('万科营业收入')