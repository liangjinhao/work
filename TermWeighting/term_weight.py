# -*- encoding: utf-8 -*-
import os
import inspect
import re
import ConfigParser
import xgboost as xgb
import numpy as np
import matplotlib.pyplot as plt
import CRFPP
from jpype import *

CONFIG_FILE = "path.conf"


class TermWeight:

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

        # CRF
        class_path = conf.get("hanlp", "classpath")
        self.model_path = home_dir + conf.get("crf", "modelpath")
        if not isJVMStarted():
            startJVM(getDefaultJVMPath(), "-Djava.class.path=" + class_path, "-Xms1g", "-Xmx1g")  # 启动JVM
        self.HanLP = JClass('com.hankcs.hanlp.HanLP')

        # 初始化 self.TFDict, self.IDFDict 与 self.EntropyDict
        with open(tf_data_path, 'rU') as f:
            for line in f:
                line = line.decode('utf-8')
                segments = line.split('\t')
                self.TFDict[segments[0]] = segments[1].strip()

        with open(idf_data_path, 'rU') as f:
            for line in f:
                line = line.decode('utf-8')
                segments = line.split('\t')
                self.IDFDict[segments[0]] = segments[1].strip()

        with open(entropy_data_path, 'rU') as f:
            for line in f:
                line = line.decode('utf-8')
                segments = line.split('\t')
                self.EntropyDict[segments[0]] = (segments[1], segments[2].strip())

        self.param = {
            'objective': "rank:pairwise",  # specify objective
            'eta': 0.13,  # step size shrinkage
            'gamma': 1,  # minimum loss reduction required to make a further partition
            'min_child_weight': 0.1,  # minimum sum of instance weight(hessian) needed in a child
            'max_depth': 4,  # maximum depth of a tree
            'num_round': 10,  # the number of round to do boosting
            'save_period': 0  # 0 means do not save any model except the final round model
        }

        self.data = 'data/TermScore.data'  # 准备好的 rank:pairwise 的数据文件

        self.train = 'data/train.data'  # 准备好的 rank:pairwise 的数据文件
        self.train_data = 'data/train'  # 分割出的训练文件
        self.train_data_group = 'data/train.group'  # 分割出的训练文件对应的group文件

        self.test = 'data/test.data'  # 准备好的用于测试的 rank:pairwise 数据文件
        self.test_data = 'data/test'   # 分割出的测试文件
        self.test_data_group = 'data/test.group'  # 分割出的测试文件对应的group文件

        # self._trans_data(self.train)
        # self._trans_data(self.test)

        self.bst = self.train_bst()
        #
        # self.tt = train_data.TrainData()

    def _trans_data(self, data_path):
        """
        转换数据，负责把人工标注的数据转换成供训练的train文件和group文件
        :param data_path: 
        :return: 
        """

        # 人工数据->
        print data_path.replace('.data', '')
        f_feature = open(data_path.replace('.data', ''), 'w')
        f_group = open(data_path.replace('.data', '') + '.group', 'w')
        line_count = 0

        pattern = re.compile('^[\S]{1,}\t[012]\n$')

        with open(data_path, 'rU') as f:
            scores = []
            words = []
            count = 0
            for line in f:
                print line_count
                line_count += 1
                if line_count == 62:
                    print 2
                if re.match(pattern, line):
                    words.append(line.split('\t')[0])
                    scores.append(line.split('\t')[1].strip())
                    count += 1
                else:
                    sentence = ''.join(words)
                    res = self.get_sentence_features(sentence)
                    if len(res) != len(words):
                        # 分词不一致，英文引起的
                        sentence = ' '.join(words)
                        res = self.get_sentence_features(sentence)
                        if res is []:
                            scores = []
                            words = []
                            count = 0
                            print '----'
                            continue
                    for i in range(len(res)):
                        new_line = scores[i]
                        features = res[i][1]
                        num = 1
                        for j in features:
                            new_line += ' ' + str(num) + ':' + str(j)
                            num += 1
                        f_feature.write(new_line + '\n')

                    f_group.write(str(count) + '\n')
                    scores = []
                    words = []
                    count = 0

    def train_bst(self):
        dtrain = xgb.DMatrix(self.train_data)
        bst = xgb.train(self.param, dtrain)
        return bst

    def predict_query(self, query):
        res = self.get_sentence_features(query)
        features = []
        words = []
        for i in res:
            words.append(i[0])
            row = [0]
            row.extend(i[1])
            features.append(row)
        test_data = np.array(features)
        dtest = xgb.DMatrix(test_data)
        ypred = self.bst.predict(dtest)
        for i in range(len(ypred)):
            print words[i], ypred[i]

    def get_sentence_features(self, sentence):

        # 分词
        sentence = sentence.decode('utf-8')
        res = self.HanLP.segment(sentence)
        char_list = []
        nature_list = []
        word_list = []
        for item in res:
            if item.word == ' ':
                continue
            word = item.word
            word_list.append(word)
            char_list.extend(list(word))
            nature = java.lang.String.valueOf(item.nature)
            nature_list.extend([nature] * len(list(word)))

        # CRF模块进行tag预测得到Tag特征
        tags_literal = []

        tagger = CRFPP.Tagger("-m " + self.model_path)
        for i in range(len(char_list)):
            tagger.add((char_list[i] + '\t' + nature_list[i]).encode('utf-8'))
        tagger.parse()
        size = tagger.size()  # token size
        tag_list = []
        for i in range(0, size):
            tag = tagger.y2(i)
            tag_list.append(tag)

        pos = 0
        for i in word_list:
            if pos > len(tag_list)-1:
                return []
            tags_literal.append(tag_list[pos])
            pos = pos + len(i)

        tags = []
        for i in tags_literal:
            if i == '1':  # 1 主体
                tags.append(1.0)
            if i == '2':  # 2 指标
                tags.append(0.8)
            if i == '3':  # 3 时间
                tags.append(0.5)
            if i == '4':  # 4 地区
                tags.append(0.6)
            if i == '5':  # 5 公式
                tags.append(0.4)
            if i == '6':  # 6 频率
                tags.append(0.3)
            if i == '7':  # 7 连词
                tags.append(0.1)
            if i == 'a':  # 7 连词
                tags.append(1.0)
            if i == 'b':  # 7 连词
                tags.append(1.0)
            if i == 'c':  # c 描述
                tags.append(0.2)
            if i == 'd':  # d 限定词
                tags.append(0.7)
            if i == 'o':  # o 无用信息
                tags.append(0.0)

        # 其他特征
        sentence_length = float(len(sentence))
        term_offset = 0
        result = []
        if len(word_list) != len(tags):
            print ''.join(word_list), tags_literal, tags
        for i in range(len(word_list)):
            term_length_ratio = len(word_list[i])/sentence_length
            term_offset_ratio = term_offset/sentence_length
            term_offset += len(word_list[i])

            tf = float(self.TFDict[word_list[i]]) if word_list[i] in self.TFDict else 0.0
            idf = float(self.IDFDict[word_list[i]]) if word_list[i] in self.IDFDict else 0.0

            l_entropy = float(self.EntropyDict[word_list[i]][0]) if word_list[i] in self.EntropyDict else 0.0
            r_entropy = float(self.EntropyDict[word_list[i]][1]) if word_list[i] in self.EntropyDict else 0.0

            perp = 0  # 语言模型
            tag = tags[i]

            features = [term_length_ratio, term_offset_ratio, tf, idf, l_entropy, r_entropy, perp, tag]
            result.append((word_list[i], features))

        return result

    def visual_statistic(self):

        dtest = xgb.DMatrix(self.test_data)
        ypred = self.bst.predict(dtest)  # 预测结果
        ymanual = []  # 标注结果
        group = []  # group信息
        with open(self.test_data) as f1, open(self.test_data + '.group') as f2:
            for line in f1:
                ymanual.append(line.split(' ')[0])
            for line in f2:
                group.append(line.strip())

        f = open(self.test)
        segs = []
        sentence_seg = []
        sentence_count = 0
        line_count = 0
        for line in f:
            print sentence_count
            if line_count in range(int(group[sentence_count])):
                sentence_seg.append(line.split('\t')[0])
                line_count += 1
            else:
                sentence_count += 1
                line_count = 0
                segs.append(sentence_seg)
                sentence_seg = []

        ypred_norm = []  # 归一化的预测结果
        max_y = 0.0
        min_y = 0.0
        for i in ypred:
            if float(i) > max_y:
                max_y = float(i)
            if float(i) < min_y:
                min_y = float(i)
        k = 2 / (max_y - min_y)
        print k, max_y, min_y
        for i in ypred:
            var = int(round(k * float(i) + 0.5))
            ypred_norm.append(var)

        # 计算整体错误率
        overall_wrong = 0
        for i in range(len(ymanual)):
            if int(ymanual[i]) != int(ypred_norm[i]):
                overall_wrong += 1
        print 'Overall Wrong Rate： ' + str(overall_wrong / float(len(ymanual))), overall_wrong, len(ymanual)

        # 计算最高的错误率
        top_wrong = 0
        offset = 0
        num = 0
        for i in group:
            sentence = ' '.join(segs[num])
            i = int(i)
            a = ymanual[offset: offset+i]
            b = ypred_norm[offset: offset+i]
            c = ypred[offset: offset+i]
            offset += i
            biggest = 0
            index = 0
            for j in range(len(a)):
                if a[j] > biggest:
                    biggest = a[j]
                    index = j
            if a[index] != 2:
                top_wrong += 1
                # print sentence
                # print a, b
                # print c
            num += 1

        print 'Top Wrong Rate： ' + str(top_wrong / float(len(group))), top_wrong, len(group)

        pass

    def plot_prediction_result(self):

        dtest = xgb.DMatrix(self.test_data)
        y = []
        f = open(self.test_data)
        for line in f:
            y.append(line.split(' ')[0])
        ypred = self.bst.predict(dtest)

        ypred2 = []
        max_y = 0.0
        min_y = 0.0
        for i in ypred:
            if float(i) > max_y:
                max_y = float(i)
            if float(i) < min_y:
                min_y = float(i)
        k = 2/(max_y-min_y)
        print k, max_y, min_y
        for i in ypred:
            var = int(round(k*float(i) + 0.5))
            ypred2.append(var)

        wrong = 0
        for i in range(len(y)):
            if int(y[i]) != int(ypred2[i]):
                wrong += 1
        print 'Wrong Rate： ' + str(wrong/float(len(y))), wrong, len(y)

        start = 0
        end = len(y)
        t1 = np.arange(0, end - start, 1)
        t2 = np.arange(0, end - start, 1)

        plt.figure(1)
        # plt.subplot(211)
        plt.plot(t1, y[start:end], '+', color='g', label='standard')

        # plt.subplot(212)
        plt.plot(t2, ypred[start:end], '1', color='r', label='origin')
        plt.plot(t2, ypred2[start:end], '2', color='b', label='norm')
        plt.grid(True)
        plt.show()

        xgb.plot_importance(self.bst)
        plt.show()

        # xgb.plot_tree(self.bst, num_trees=1)
        # plt.show()

a = TermWeight()
# print a.predict_query('NAND Flash 價 格')
print a.visual_statistic()
print a.plot_prediction_result()

