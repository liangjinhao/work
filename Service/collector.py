import os
import inspect
import re
import configparser
import hanlp_segmentor
import term_ranking
import ac_search
import time_extractor
from collections import Counter

CONFIG_FILE = "path.conf"


class Collector:
    """
    Collector 类负责把 NLP 模块分析的东西全部集合在一起，封装成Json对象返回给调用对象
    """

    def __init__(self):
        self.term_rank = term_ranking.TermRank()
        self.segmentor = hanlp_segmentor.HanlpSegmentor()
        self.ahocorasick = ac_search.ACSearch()

        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)
        useless_dict_path = home_dir + conf.get("dictionary", "norm_useless")
        self.ahocorasick.add_dict(useless_dict_path)
        self.ahocorasick.start()
        self.te = time_extractor.TimeExtractor()

        weight_drop = home_dir + conf.get("dictionary", "weight_drop")
        self.weight_drop = set()
        with open(weight_drop) as f:
            for line in f:
                self.weight_drop.add(line.strip('\n'))

        self.phrase_dict_path = home_dir + conf.get("dictionary", "phrase")
        self.phrase_dict = dict()
        self.reload_dict()

    def reload_dict(self):
        self.phrase_dict = dict()
        with open(self.phrase_dict_path) as f:
            for line in f:
                if not line.startswith('#') and line != '\n':
                    self.phrase_dict[line.strip('\n').split('\t')[0]] = line.strip('\n').split('\t')[-1]

    def dict_merge(self, arg):
        """
        按照字典里给出的词，将现有的分词结果进行合并，合并是正向最大合并。比如对于‘红杉 资本 回本率’这个分词结果，
        如果字典里定义了‘红杉资本’和‘资本回本率’，则最终结果变成‘红杉资本 回本率’
        :param arg:
        :return:
        """
        crf_result = arg['data']
        xgboost_result = arg['term_weight']
        new_crf_result = []
        new_xgboost_result = []
        head = 0
        rear = len(crf_result)
        for i in range(head, rear):
            for j in range(rear - head):
                temp_word = ''.join([x['term'] for x in crf_result][head:rear])
                temp_nature = []
                [temp_nature.extend([x['pos']] * len(x['term'])) for x in crf_result[head:rear]]
                temp_tag = []
                [temp_tag.extend([x['type']] * len(x['term'])) for x in crf_result[head:rear]]
                temp_weight = []
                [temp_weight.extend([x['weight']] * len(x['term'])) for x in xgboost_result[head:rear]]
                if rear - head == 1:
                    word = temp_word
                    nature = Counter(temp_nature).most_common(1)[0][0]
                    if word in self.phrase_dict:
                        tag = self.phrase_dict[word]
                    else:
                        tag = Counter(temp_tag).most_common(1)[0][0]
                    sum_weight = 0
                    for w in temp_weight:
                        sum_weight += w
                    if word in self.phrase_dict and tag == 'useless':
                        weight = float(0.1)
                    elif word in self.phrase_dict and tag.startswith('subject'):
                        weight = float(2.0)
                    else:
                        weight = sum_weight / (rear - head)
                    new_crf_result.append({'pos': nature, 'term': word, 'type': tag})
                    new_xgboost_result.append({'term': word, 'weight': weight})
                    head += (rear - head)
                elif temp_word in self.phrase_dict:
                    word = temp_word
                    nature = Counter(temp_nature).most_common(1)[0][0]
                    if word in self.phrase_dict:
                        tag = self.phrase_dict[word]
                    else:
                        tag = Counter(temp_tag).most_common(1)[0][0]
                    sum_weight = 0
                    for w in temp_weight:
                        sum_weight += w
                    if tag == 'useless':
                        weight = float(0.1)
                    elif word in self.phrase_dict and tag.startswith('subject'):
                        weight = float(2.0)
                    else:
                        weight = sum_weight / (rear - head)
                    new_crf_result.append({'pos': nature, 'term': word, 'type': tag})
                    new_xgboost_result.append({'term': word, 'weight': weight})
                    head += (rear - head)
                    break
                rear -= 1

            rear = len(crf_result)

        result = {'data': new_crf_result, 'term_weight': new_xgboost_result}
        return result

    def collect(self, sentence):
        """
        返回现有的 NLP 模块对某个句子的分析结果（Json格式）
        :param sentence: 输入的一个句子
        :return: 
        """

        # 定义返回的Json对象格式
        final_result = dict({
            "data": [],   # 原句子的CRF序列标注结果
            "title": "",  # 输入的原句子
            "brief": "",  # 句子的简要版本，即去掉句子里无用的一些字词等（这些无用字词由字典给出）
            "years": "",  # 句子里包含的年份信息
            "term_weight": []  # 句子中各个词语的ranking
        })
        final_result["title"] = sentence

        # 处理词典中调整的词的tag
        tr_res = self.term_rank.predict_query(sentence)
        tr_res = self.dict_merge(tr_res)

        final_result["term_weight"] = tr_res["term_weight"]

        # 处理data
        crf_result = tr_res["data"]
        for i in range(len(crf_result)):
            var = crf_result[i]
            # 英文的识别很差，先统一看成主体
            if re.match(r'[a-zA-Z]+', var['term']):
                crf_result[i]['type'] = 'subject1'
                final_result["data"].append({'term': var['term'], 'type': 'subject'})
            else:
                final_result["data"].append({'term': var['term'], 'type': var['type']})

        # 处理brief
        brief = ''
        for i in range(len(crf_result)):
            if crf_result[i]['term'] not in self.phrase_dict:
                brief += crf_result[i]['term']
        final_result["brief"] = brief

        # 处理时间
        final_result['years'] = self.te.extract(sentence)
        print(final_result)
        return final_result


# a = Collector()
# a.collect('ofo摩拜新增用户量走势')
