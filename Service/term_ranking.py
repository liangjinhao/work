# -*- encoding: utf-8 -*-
import os
import inspect
import re
import configparser
import xgboost as xgb
import numpy as np
import math
from sklearn.metrics import *
import crf_plus as crf
from collections import Counter
from gensim.models import Word2Vec

CONFIG_FILE = "path.conf"


class TermRank:

    def __init__(self):

        # CRF预测的Tag 映射成离散值(0.0~1.0)
        self.tag_mapping = {'subject1': 1.0, 'subject2': 1.0, 'subject3': 1.0, 'indicator': 0.8, 'product': 0.9,
                            'area': 0.7, 'formula': 0.4, 'useless': 0.0, 'time': 0.6}

        # hanlp pos词性 映射成离散值(0~9)
        self.pos_mapping = {'a': 2, 'ad': 2, 'ag': 4, 'al': 4, 'an': 3, 'b': 6, 'begin': 0, 'bg': 4, 'bl': 4, 'c': 1,
                            'cc': 1, 'd': 2, 'dg': 2, 'dl': 6, 'e': 0, 'end': 0, 'f': 6, 'g': 9, 'gb': 9, 'gbc': 9,
                            'gc': 9, 'gg': 9, 'gi': 9, 'gm': 7, 'gp': 7, 'h': 4, 'i': 4, 'j': 9, 'k': 4, 'l': 4,
                            'm': 1, 'mg': 7, 'Mg': 7, 'mq': 2, 'n': 7, 'nb': 9, 'nba': 9, 'nbc': 9, 'nbp': 9, 'nf': 9,
                            'ng': 7, 'nh': 9, 'nhd': 9, 'nhm': 9, 'ni': 9, 'nic': 9, 'nis': 9, 'nit': 9, 'nl': 7,
                            'nm': 9, 'nmc': 9, 'nn': 8, 'nnd': 8, 'nnt': 8, 'nr': 9, 'nr1': 9, 'nr2': 9, 'nrf': 9,
                            'nrj': 9, 'ns': 7, 'nsf': 7, 'nt': 9, 'ntc': 9, 'ntcb': 9, 'ntcf': 9, 'ntch': 9, 'nth': 9,
                            'nto': 9, 'nts': 9, 'ntu': 9, 'nx': 9, 'nz': 9, 'o': 7, 'p': 1, 'pba': 1, 'pbei': 1,
                            'q': 1, 'qg': 1, 'qt': 6, 'qv': 6, 'r': 6, 'rg': 2, 'Rg': 2, 'rr': 2, 'ry': 2, 'rys': 2,
                            'ryt': 2, 'ryv': 2, 'rz': 3, 'rzs': 7, 'rzt': 6, 'rzv': 4, 's': 7, 't': 6, 'tg': 6, 'u': 2,
                            'ud': 2, 'ude1': 1, 'ude2': 1, 'ude3': 1, 'udeng': 1, 'udh': 1, 'ug': 1, 'uguo': 1,
                            'uj': 2, 'ul': 2, 'ule': 1, 'ulian': 1, 'uls': 1, 'usuo': 4, 'uv': 1, 'uyy': 1, 'uz': 1,
                            'uzhe': 1, 'uzhi': 1, 'v': 5, 'vd': 4, 'vf': 4, 'vg': 6, 'vi': 5, 'vl': 4, 'vn': 7,
                            'vshi': 1, 'vx': 5, 'vyou': 1, 'w': 0, 'wb': 0, 'wd': 0, 'wf': 0, 'wh': 0, 'wj': 0,
                            'wky': 0, 'wkz': 0, 'wm': 0, 'wn': 0, 'wp': 0, 'ws': 0, 'wt': 0, 'ww': 0, 'wyy': 0,
                            'wyz': 0, 'x': 7, 'xu': 7, 'xx': 3, 'y': 1, 'yg': 1, 'z': 2, 'zg': 2}

        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)

        # 初始化 self.TFDict, self.IDFDict 与 self.EntropyDict
        self.TFDict = {}
        self.IDFDict = {}
        self.EntropyDict = {}
        tf_data_path = home_dir + conf.get("data", "TF_data")  # 词频数据路径
        idf_data_path = home_dir + conf.get("data", "IDF_data")  # 词IDF数据路径
        entropy_data_path = home_dir + conf.get("data", "Entropy_data")  # 词左右熵数据路径
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

        # 设置CRF
        self.crf = crf.CRF()

        # 设置Word2Vec
        self.model_path = home_dir + conf.get("word2vec", "model")
        self.model = Word2Vec.load(self.model_path)

        # 设置XGBoost
        self.param = {
            'learning_rate': 0.1,
            'objective': "rank:pairwise",  # specify objective
            'eval_metric': 'ndcg',
            # 'eta': 0.1,  # step size shrinkage
            'gamma': 0,  # minimum loss reduction required to make a further partition
            'min_child_weight': 6,  # minimum sum of instance weight(hessian) needed in a child
            'max_depth': 14,  # maximum depth of a tree
            'num_round': 200,  # the number of round to do boosting
            'save_period': 0,  # 0 means do not save any model except the final round model
            'n_estimators': 1000,
            'subsample': 1,
            'colsample_bytree': 0.8,
            'scale_pos_weight': 1,
            'lambda': 0.9,
            'alpha': 0.1
        }

        self.data = home_dir + conf.get("xgboost", "data")  # 人工标注的数据文件

        self.data_train = home_dir + conf.get("xgboost", "data_train")  # 人工标注的数据文件中分出的训练数据
        self.train = home_dir + conf.get("xgboost", "train")    # xgboost训练数据
        self.train_group = self.train + '.group'  # xgboost训练数据对应的group文件

        self.data_test = home_dir + conf.get("xgboost", "data_test")  # 人工标注的数据文件中分出的测试数据
        self.test = home_dir + conf.get("xgboost", "test")     # xgboost测试数据
        self.test_group = self.test + '.group'  # xgboost测试数据对应的group文件

        # self.split_data(0.2)  # 切分人工标注的数据文件为训练数据和测试数据
        # self._trans_data(self.data_train, self.train, self.train_group)  # 得到训练数据的feature数据和group数据
        # self._trans_data(self.data_test, self.test, self.test_group)  # 得到测试数据的feature数据和group数据

        self.xgboost_model_path = home_dir + conf.get("xgboost", "model")
        # self.bst = self.train_bst()

        self.bst = xgb.Booster({'nthread': 4})  # init model
        self.bst.load_model(self.xgboost_model_path)  # load model

        # print('-----test-----')
        # self.visual_statistic(self.test, self.data_test)
        # print('-----train-----')
        # self.visual_statistic(self.train, self.data_train)

    def train_bst(self):
        """
        训练XGBoost模型
        :return: 
        """
        dtrain = xgb.DMatrix(self.train)
        bst = xgb.train(self.param, dtrain)
        bst.save_model(self.xgboost_model_path)
        return bst

    def predict_query(self, query):
        """
        预测一个句子的CRF标注结果和TermRanking结果，返回格式为Json
        :param query: 一个句子
        :return: 
        """
        res, crf_res = self.get_sentence_features(query)
        features = []
        words = []
        for i in res:
            words.append(i[0])
            row = [0]
            row.extend(i[1:])
            features.append(row)
        test_data = np.array(features)
        dtest = xgb.DMatrix(test_data)
        ypred = self.bst.predict(dtest)
        result = []
        for i in range(len(ypred)):
            result.append({'term': words[i], 'weight': float(ypred[i])})

        final_result = dict({
            "data": crf_res,
            "term_weight": result
        })

        return final_result

    def get_sentence_features(self, sentence):
        """
        返回一个句子的所有特征和该句子的CRF标注结果
        :param sentence: 
        :return: 
        """
        word_list = []  # 分词得到的词list
        pos = []  # 分词得到的词的词性
        tags = []  # 分词得到的词的crf标签
        crf_res = self.crf.extract(sentence)
        for item in crf_res:
            word_list.append(item['term'])
            pos.append(item['pos'])
            tags.append(item['type'])

        # 特征
        # term_len 1: Term长度
        # term_len_ratio 2：Term长度比例
        # term_offset 3：Term偏移量
        # term_offset_ratio 4：Term偏移量比例
        # begin 5：Term是否在句首
        # end 6：Term是否在句尾

        # pos 7：Term的词性
        # 1_pos 8：Term的前一个Term的词性
        # 2_pos 9：Term的前两个Term的词性
        # pos_1 10：Term的后一个Term的词性
        # pos_2 11：Term的后两个Term的词性

        # tf 12：Term的TF
        # idf 13：Term的IDF
        # le 14: Term的左熵
        # re 15：Term的右熵
        # 1_re 16：Term的前一个Term的右熵
        # le_1 17：Term的后一个Term的左熵

        # tag 18：Term的CRF标签
        # tag_ratio 19：Term的CRF标签占同类型标签的比例
        # 1_tag 20：Term的前一个Term的CRF标签
        # 2_tag 21：Term的前两个Term的CRF标签
        # tag_1 22：Term的后一个Term的CRF标签
        # tag_2 23：Term的后两个Term的CRF标签
        # tag_num 35：Term前面各种CRF标签的数目[12]

        tag_count = {}
        base_features = []
        for i in range(len(word_list)):
            base_feature = list()  # 'term_len', 'term_offset', 'pos', 'tf', 'idf', 'le', 're', 'tag'
            base_feature.append(len(word_list[i]))
            base_feature.append(i)
            base_feature.append(self.pos_mapping[pos[i]])
            base_feature.append(math.log(float(self.TFDict[word_list[i]]) if word_list[i] in self.TFDict else 1))
            base_feature.append(float(self.IDFDict[word_list[i]]) if word_list[i] in self.IDFDict else 0.0)
            base_feature.append(float(self.EntropyDict[word_list[i]][0]) if word_list[i] in self.EntropyDict else 0.0)
            base_feature.append(float(self.EntropyDict[word_list[i]][1]) if word_list[i] in self.EntropyDict else 0.0)
            base_feature.append(tags[i])
            base_features.append(base_feature)

            tag_count[tags[i]] = tag_count[tags[i]]+1 if tags[i] in tag_count else 1

        sentence_length = float(len(sentence))

        full_features = []
        tag_acu_count = {}
        for i in range(len(base_features)):
            base_feature = base_features[i]
            full_feature = list()

            full_feature.append(word_list[i])  # 词

            full_feature.append(base_feature[0])  # 1: Term长度
            full_feature.append(base_feature[0]/float(sentence_length))  # 2：Term长度比例
            full_feature.append(base_feature[1])  # 3：Term偏移量
            full_feature.append(base_feature[1]/float(sentence_length))  # 4：Term偏移量比例
            full_feature.append(1 if base_feature[1] == 0 else 0)  # 5：Term是否在句首
            full_feature.append(1 if base_feature[1] == sentence_length-1 else 0)  # 6：Term是否在句尾
            full_feature.append(base_feature[2])  # 7：Term的词性
            full_feature.append(base_features[i-1][2] if i-1 >= 0 else 0)  # 8：Term的前一个Term的词性
            full_feature.append(base_features[i-2][2] if i-2 >= 0 else 0)  # 9：Term的前两个Term的词性
            full_feature.append(base_features[i+1][2] if i+1 < len(base_features) else 0)  # 10：Term的后一个Term的词性
            full_feature.append(base_features[i+2][2] if i+2 < len(base_features) else 0)  # 11：Term的后两个Term的词性

            full_feature.append(base_feature[3])  # 12：Term的TF
            full_feature.append(base_feature[4])  # idf 13：Term的IDF
            full_feature.append(base_feature[5])  # 14: Term的左熵
            full_feature.append(base_feature[6])  # 15：Term的右熵
            full_feature.append(base_features[i-1][6] if i-1 >= 0 else 0)  # 16：Term的前一个Term的右熵
            full_feature.append(base_features[i+1][5] if i+1 < len(base_features) else 0)  # 17：Term的后一个Term的左熵

            full_feature.append(self.tag_mapping[base_feature[7]])  # 18：Term的CRF标签
            full_feature.append(1/float(tag_count[base_feature[7]]))  # 19：Term的CRF标签占同类型标签的比例
            full_feature.append(self.tag_mapping[base_features[i-1][7]] if i-1 >= 0 else 0.0)  # 20：Term的前一个Term的CRF标签
            full_feature.append(self.tag_mapping[base_features[i-2][7]] if i-2 >= 0 else 0.0)  # 21：Term的前两个Term的CRF标签
            full_feature.append(self.tag_mapping[base_features[i+1][7]] if i+1 < len(base_features) else 0.0)  # 22：Term的后一个Term的CRF标签
            full_feature.append(self.tag_mapping[base_features[i+2][7]] if i+2 < len(base_features) else 0.0)  # 23：Term的后两个Term的CRF标签

            # 35：Term前面各种CRF标签的数目[12]
            tag_acu_count[base_feature[7]] = tag_acu_count[base_feature[7]]+1 if base_feature[7] in tag_acu_count else 1
            for j in sorted(self.tag_mapping.keys(), reverse=True):
                full_feature.append(tag_acu_count[j] if j in tag_acu_count else 0)

            # 36~86 Word2Vec
            if word_list[i] in self.model:
                full_feature.extend(self.model[word_list[i]])
            else:
                print("Not in model:" + word_list[i])
                full_feature.extend([0]*50)

            full_features.append(full_feature)

        return full_features, crf_res

    def _trans_data(self, data_path, features, group):
        """
        转换数据，负责把人工标注的数据转换成供训练的train文件和group文件
        :param data_path: 
        :return: 
        """
        f_feature = open(features, 'w')
        f_group = open(group, 'w')
        line_count = 0

        pattern = re.compile('^[\S]+\t[012]\n$')

        with open(data_path, 'rU') as f:
            scores = []
            words = []
            count = 0
            for line in f:
                line_count += 1
                if line_count % 1000 == 0:
                    print(line_count)
                if re.match(pattern, line):
                    words.append(line.split('\t')[0])
                    scores.append(line.split('\t')[1].strip('\n'))
                    count += 1
                else:
                    sentence = ''.join(words)
                    if len(sentence) > 120:
                        scores = []
                        words = []
                        count = 0
                        print('--太长,忽略掉--', sentence)
                        continue
                    res, crf_res = self.get_sentence_features(sentence)
                    if len(res) != len(words):
                        # 标注数据的分词（比较老旧）和hanlp的分词可能不一致，这种情况下使用hanlp的分词，采用多数表决策略决定hanlp的分词
                        # 对应的score。比如标注数据的分词和打分是['万科':'2', '净':'1', '收入':'1']，hanlp的分词是['万科', '净收入']，
                        # 那么此时按字的多数表决策略（'净','收','入'三个字都是'1'），hanlp的分词的打分是 ['万科':'2', '净收入':'1']
                        characters_tag = []
                        revised_scores = []
                        revised_words = []
                        offset = 0
                        for i in range(len(words)):
                            characters_tag.extend([scores[i]]*len(words[i]))
                        for j in range(len(crf_res)):
                            term = crf_res[j]['term']
                            revised_words.append(term)
                            revised_scores.append(Counter(characters_tag[offset:offset+len(term)]).most_common(1)[0][0])
                            offset += len(term)
                        print('--分词与原标注分词不一样--', sentence)
                        print(words, revised_words)
                        print(scores, revised_scores)
                        scores = revised_scores
                        words = revised_words

                    for i in range(len(res)):
                        new_line = scores[i]
                        features = res[i][1:]
                        num = 1
                        for j in features:
                            new_line += ' ' + str(num) + ':' + str(j)
                            num += 1
                        f_feature.write(new_line + '\n')

                    f_group.write(str(count) + '\n')
                    scores = []
                    words = []
                    count = 0

    def visual_statistic(self, data, raw):
        # data = self.test
        # raw = self.data_test
        dtest = xgb.DMatrix(data)
        ypred = self.bst.predict(dtest)  # 预测结果

        ymanual = []  # 标注结果

        group = []
        with open(data) as f1, open(data + '.group') as f2:
            for line in f1:
                ymanual.append(int(line.split(' ')[0]))
            for line in f2:
                group.append(int(line.strip()))

        f = open(raw)
        segs = []
        sentence_seg = []
        sentence_count = 0
        line_count = 0
        for line in f:
            if sentence_count == len(group):
                break
            if line_count in range(int(group[sentence_count])):
                sentence_seg.append(line.split('\t')[0])
                line_count += 1
            else:
                sentence_count += 1
                line_count = 0
                segs.append(sentence_seg)
                sentence_seg = []

        # 计算最高的错误率
        TP = 0  # 最重要的词被识别出最重要的词的个数
        FP = 0  # 不是最重要的词被识别出最重要的词的个数
        FN = 0  # 最重要的词被识别出不是最重要的词的个数

        all_sum = 0.0
        offset = 0
        count = 0
        f = open('wrong_result', 'w')
        for i in group:
            pred = ypred[offset:offset+i]
            test = ymanual[offset:offset+i]
            all_sum += self.nDCG(test, pred)
            if self.nDCG(test, pred) < 0.8:
                string = ''
                for k in range(len(test)):
                    string += segs[count][k] + '\t' + str(test[k]) + '\t' + str(pred[k]) + '\n'
                # print(string)
            offset += i
            pred_bigest = sorted(pred, reverse=True)[0]
            test_bigest = sorted(test, reverse=True)[0]
            pred_bigest_index = set()
            test_bigest_index = set()
            for j in range(len(pred)):
                if pred[j] == pred_bigest:
                    pred_bigest_index.add(j)
                if test[j] == test_bigest:
                    test_bigest_index.add(j)
            if pred_bigest_index.issubset(test_bigest_index):
                # TP += len(pred_bigest_index)
                pass
            else:
                string = ''
                for k in range(len(test)):
                    string += segs[count][k] + '\t' + str(test[k]) + '\t' + str(pred[k]) + '\n'
                string += '\n'
                f.write(string)
            TP += len(pred_bigest_index.intersection(test_bigest_index))
            FP += len(pred_bigest_index - pred_bigest_index.intersection(test_bigest_index))
            FN += len(test_bigest_index - pred_bigest_index.intersection(test_bigest_index))
            count += 1
        print('NDCG:', all_sum/count)
        precision = float(TP)/(TP+FP)
        recall = float(TP) / (TP + FN)
        f = 2*precision*recall/(precision+recall)
        print('precision = ', precision)
        print('recall = ', recall)
        print('f = ', f)

        print('R2 Value: ' + str(r2_score(ymanual, ypred)))

        # xgb.plot_importance(self.bst)
        # plt.show()

    @staticmethod
    def nDCG(original_list, predict_list):
        def DCG(list):
            sum = 0.0
            for i in range(len(list)):
                sum += pow(4, list[i] - 1) / math.log(i + 2, 2)
            return sum

        ideal_list = sorted(original_list, reverse=True)
        predict_index_list = sorted(range(len(predict_list)), key=lambda k: predict_list[k], reverse=True)
        fact_list = [original_list[i] for i in predict_index_list]
        a = DCG(fact_list)
        b = DCG(ideal_list)
        return 1 - abs(a - b) / b

    @staticmethod
    def exam_crf_train_file(file_path):
        """
        检查CRF训练文件是否符合规范
        :param file_path: 
        :return: 
        """

        pattern_string = u'^[\S\s]{1,30}\t[012]$'
        # pattern_string = u'^[\S]{1,30}\t([\d]|[a-z]|(10))$'
        pattern = re.compile(pattern_string)

        i = 0
        is_wrong = False
        message = ''
        f2 = file_path + '_new'
        f_2 = open(f2, 'w')
        f3 = file_path + '_wrong'
        f_3 = open(f3, 'w')
        with open(file_path, 'rU') as f:
            for line in f:
                i = i + 1
                if line == '\n' or line == '\r\n':
                    f_2.write(line)
                    continue
                if re.match(pattern, line):
                    f_2.write(line)
                    continue
                else:
                    is_wrong = True
                    f_3.write(line)
                    message += 'Line:' + str(i) + '\t' + line
        if is_wrong:
            print('文件：' + file_path + '存在格式错误,请查看：' + file_path + '_格式检测结果')
            f_write = open(file_path + '_格式检测结果', 'w')
            f_write.write(message)
            f_write.close()

    def split_data(self, ratio):
        """
        把训练文件按比例分割成两份
        :param ratio: 
        :return: 
        """
        import random
        file_path = self.data
        train_path = self.data_train
        test_path = self.data_test

        # 先计算出所有句子的数量，并确定抽取的句子
        line_count = 0
        with open(file_path) as f:
            for line in f:
                if line == '\n':
                    line_count += 1
        f.close()

        num_list = [n for n in range(0, line_count)]
        sample_list = random.sample(num_list, int(line_count * ratio))
        print(sample_list)
        current_sentence = 0
        train_text = ''
        test_text = ''

        with open(file_path) as f:
            for line in f:
                if current_sentence in sample_list:
                    test_text += line
                else:
                    train_text += line
                if line == '\n':
                    current_sentence += 1

        f = open(train_path, 'w')
        f.write(train_text)
        f.close()

        f = open(test_path, 'w')
        f.write(test_text)
        f.close()

# a = TermRank()
# print(a.predict_query('万科上季度收入增长情况'))
