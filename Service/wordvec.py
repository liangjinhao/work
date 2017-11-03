from gensim.models import Word2Vec
import inspect
import configparser
import os
import hanlp_segmentor
import time
import numpy as np

CONFIG_FILE = "path.conf"


class ABCWordVec:
    """
    训练Word2Vec模型
    """

    def __init__(self):
        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)
        self.vec_dimension = 50  # Word2Vec模型的维数
        self.sentences_path = home_dir + conf.get("word2vec", "sentences")  # 训练Word2Vec模型的训练句子（分完词的句子）路径
        self.model_path = home_dir + conf.get("word2vec", "model")  # 存放Word2Vec的路径
        self.model = Word2Vec.load(self.model_path)

    def train_model(self):
        """
        训练一个Word2Vec模型
        :return: 
        """
        print('Start to train:\t' + time.asctime(time.localtime(time.time())))
        sentences = MySentences(self.sentences_path)
        model = Word2Vec(sentences, size=self.vec_dimension, window=8, min_count=3, workers=4)
        model.save(self.model_path)

    def get_word_vec(self, word):
        """
        返回一个词的Word2Vec向量，如果Word2Vec里面没有这个词，返回零向量
        :param word: 一个词
        :return: 
        """
        if word in self.model:
            return self.model[word]
        else:
            print('Not in Word2vec: ' + word)
            return np.array([0.0] * self.vec_dimension, dtype='float32')


class MySentences(object):
    def __init__(self, sentences_path):
        self.segmentor = hanlp_segmentor.HanlpSegmentor()
        self.sentences_path = sentences_path

    def __iter__(self):
        for line in open(self.sentences_path):
            yield line.strip('\n').split(' ')

# a = ABCWordVec()
# print(a.get_word_vec('收入'))
