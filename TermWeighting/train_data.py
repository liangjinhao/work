# -*- coding: utf-8 -*-
import sys
import os
import inspect
import time
import re
import commands
import ConfigParser
import CRFPP
import hanlp_segmentor
from jpype import *

reload(sys)
sys.setdefaultencoding('utf-8')

CONFIG_FILE = "path.conf"


class TrainData:

    def __init__(self):
        """
        训练数据的格式，文本特征(QueryLength TermLength TermOffset )，统计特征(TF IDF TF-IDF LE RE)，语言特征(PerplexityRatio)，Chunk特征(Tag)
        <Score QueryLength TermLength TermOffset TF IDF TF-IDF LE RE PerplexityRatio Tag>
        """

        self.TFDict = {}
        self.IDFDict = {}
        self.EntropyDict = {}

        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        conf = ConfigParser.ConfigParser()
        conf.read(CONFIG_FILE)

        tf_data_path = home_dir + conf.get("data", "TF_data")  # 词频数据路径
        idf_data_path = home_dir + conf.get("data", "IDF_data")  # 词IDF数据路径
        entropy_data_path = home_dir + conf.get("data", "Entropy_data")  # 词左右熵数据路径

        self.lm_path = home_dir + conf.get("srilm", "modelpath")
        self.test_data = home_dir + conf.get("srilm", "testdata")
        self.segmentor = hanlp_segmentor.HanLPSegmentor()

        # CRF
        class_path = conf.get("hanlp", "classpath")
        self.model_path = home_dir + conf.get("crf", "modelpath")
        if not isJVMStarted():
            startJVM(getDefaultJVMPath(), "-Djava.class.path=" + class_path, "-Xms1g", "-Xmx1g")  # 启动JVM
        self.HanLP = JClass('com.hankcs.hanlp.HanLP')

        # 初始化 self.TFDict, self.IDFDict 与 self.EntropyDict
        with open(tf_data_path, 'rU') as f:
            for line in f:
                segments = line.split('\t')
                self.TFDict[segments[0]] = segments[1].strip()

        with open(idf_data_path, 'rU') as f:
            for line in f:
                segments = line.split('\t')
                self.IDFDict[segments[0]] = segments[1].strip()

        with open(entropy_data_path, 'rU') as f:
            for line in f:
                segments = line.split('\t')
                self.EntropyDict[segments[0]] = (segments[1], segments[2].strip())

    def process_train_data(self):
        """
        score qid 1:query_length
        <Score QueryLength TermLength TermOffset TF IDF TF-IDF LE RE PerplexityRatio Tag>
        2 qid:11909 1:QueryLength 2:TermLength 3:TermOffset 4:TF 5:IDF 6:TF-IDF 7:LE 8:RE 9:PerplexityRatio 10:Tag
        :return: 
        """
        # Raw数据: Term - Score
        raw_data = './data/TermScore.data'
        train_data = './data/rank_pairwise.test.data'
        pattern = re.compile(u'^[\S]{1,10}\t[012]$')

        with open(raw_data, 'rU') as f, open(train_data, 'w') as f2:
            qid = 1
            sentence = []
            scores = []
            for line in f:
                if re.match(pattern, line):
                    sentence.append(line.split('\t')[0])
                    scores.append(line.split('\t')[1].strip())
                else:
                    if len(sentence) == 0:
                        continue
                    tags = self.get_crf_tag(''.join(sentence))
                    query_length = len(''.join(sentence))
                    for i in range(len(sentence)):
                        score = scores[i]
                        term_length = len(str(sentence[i]))
                        term_offset = i
                        tf = float(self.TFDict[str(sentence[i])]) if str(sentence[i]) in self.TFDict else 0.0
                        idf = float(self.IDFDict[str(sentence[i])]) if str(sentence[i]) in self.IDFDict else 0.0
                        tfidf = tf*idf
                        l_entropy = self.EntropyDict[str(sentence[i])][0] if str(sentence[i]) in self.EntropyDict else 0.0
                        r_entropy = self.EntropyDict[str(sentence[i])][1] if str(sentence[i]) in self.EntropyDict else 0.0
                        perp = ''  # 语言模型
                        tag = ord(tags[i])
                        new_line = str(score) + ' ' + \
                            'qid:' + str(qid) + ' ' + \
                            '1:' + str(query_length) + ' ' + \
                            '2:' + str(term_length) + ' ' + \
                            '3:' + str(term_offset) + ' ' + \
                            '4:' + str(tf) + ' ' + \
                            '5:' + str(idf) + ' ' + \
                            '6:' + str(tfidf) + ' ' + \
                            '7:' + str(l_entropy) + ' ' + \
                            '8:' + str(r_entropy) + ' ' + \
                            '10:' + str(tag) + '\n'
                        print tag
                        f2.write(new_line)

                    qid += 1
                    sentence = []
                    scores = []

    def get_srilm_perplexity(self, sentence):
        """
        使用SRILM工具训练的语言模型计算当前句子的 perplexity
        :param sentence: 
        :return: 
        """
        start = time.time()
        seged_sentence = ' '.join(self.segmentor.get_hanlp_segment(sentence))
        f = open(self.test_data, 'w')
        f.write(seged_sentence)
        f.close()
        print '-1--', time.time() - start

        # ngram -ppl test.data -order 3 -lm train.lm
        command = 'ngram -ppl ' + self.test_data + ' -order 2 -lm ' + self.lm_path
        start = time.time()
        state, output = commands.getstatusoutput(command)
        print '-2--', time.time() - start
        print output
        # file test.data3: 1 sentences, 3 words, 0 OOVs 0 zeroprobs, logprob= -10.0964 ppl= 334.277 ppl1= 2319.93

        pass

    def get_crf_tag(self, sentence):
        """
        使用CRF模块识别出句子的各个term的chunk tag
        :param sentence: 
        :return: 
        """
        tagger = CRFPP.Tagger("-m " + self.model_path)
        res = self.HanLP.segment(sentence)
        item_list = []
        word_list = []
        pos_list = []
        for item in res:
            word = item.word
            item_list.append(word)
            nature = java.lang.String.valueOf(item.nature)
            word_list.extend(list(word))
            pos_list.extend([nature] * len(list(word)))

        for i in range(len(word_list)):
            tagger.add((word_list[i] + '\t' + pos_list[i]).encode('utf-8'))

        tagger.parse()  # parse and change internal stated as 'parsed'
        size = tagger.size()  # token size
        xsize = tagger.xsize()  # column size
        tag_list = []
        for i in range(0, size):
            char = tagger.x(i, 0).decode('utf-8')
            tag = tagger.y2(i)
            tag_list.append(tag)

        pos = 0
        tags = []
        for i in item_list:
            tags.append(tag_list[pos])
            pos = pos + len(i)

        return tag_list

    def get_sentence_features(self, sentence):
        sentence = sentence.encode('utf-8')
        segs = self.segmentor.get_hanlp_segment(sentence)
        tags = self.get_crf_tag(''.join(segs))
        query_length = len(''.join(segs))
        result = []
        for i in range(len(segs)):
            term_length = len(str(segs[i]))
            term_offset = i
            tf = float(self.TFDict[str(segs[i])]) if str(segs[i]) in self.TFDict else 0.0
            idf = float(self.IDFDict[str(segs[i])]) if str(segs[i]) in self.IDFDict else 0.0
            tfidf = tf * idf
            l_entropy = self.EntropyDict[str(segs[i])][0] if str(segs[i]) in self.EntropyDict else 0.0
            r_entropy = self.EntropyDict[str(segs[i])][1] if str(segs[i]) in self.EntropyDict else 0.0
            perp = ''  # 语言模型
            tag = ord(tags[i])

            features = [2, query_length, term_length, term_offset, tf, idf, tfidf, float(l_entropy), float(r_entropy), 0, tag]
            result.append((segs[i], features))
        return result

a = TrainData()
print a.get_crf_tag('单晶硅电池(125mmx125mm)')