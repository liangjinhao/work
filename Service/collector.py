import os
import inspect
import re
import configparser
import hanlp_segmentor
import term_ranking
import ac_search
import abc_time
import update_dic
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

        self.te = abc_time.ABCYear()

        self.phrase_dict_path = home_dir + conf.get("dictionary", "phrase")
        self.phrase_dict = dict()
        self.reload_dict()

    def reload_dict(self):
        self.phrase_dict = dict()
        if os.path.exists(self.phrase_dict_path + '_local'):
            with open(self.phrase_dict_path + '_local') as f:
                for line in f:
                    if not line.startswith('#') and line != '\n':
                        self.phrase_dict[line.strip('\n').split('\t')[0]] = line.strip('\n').split('\t')[-1]
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
                    elif word in self.phrase_dict and tag.startswith('indicator'):
                        weight = float(1.2)
                    else:
                        weight = sum_weight / len(word)
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
                    elif word in self.phrase_dict and tag.startswith('indicator'):
                        weight = float(1.2)
                    else:
                        weight = sum_weight / len(word)
                    # 在这种合并的词前后加上 双引号
                    new_crf_result.append({'pos': nature, 'term': '"'+word+'"', 'type': tag})
                    new_xgboost_result.append({'term': '"'+word+'"', 'weight': weight})
                    head += (rear - head)
                    break
                rear -= 1

            rear = len(crf_result)

        result = {'data': new_crf_result, 'term_weight': new_xgboost_result}
        return result

    def tune_crf_tag(self, arg):
        """
        根据xgboost的预测权重值来调整CRF的tag。
        当xgboost的预测权重值大于0.3时，而CRF的标签为useless时，将CRF的标签修为subject4;
        当xgboost的预测权重值小于0.2时，而CRF的标签不为useless时，将CRF的标签修为useless;
        :param arg:
        :return:
        """
        crf_result = arg['data']
        xgboost_result = arg['term_weight']
        new_crf_result = []

        for i in range(len(crf_result)):
            crf_tag = crf_result[i]['type']
            weight = xgboost_result[i]['weight']
            if crf_tag == 'useless' and weight > 0.3:
                new_crf_result.append({'pos': crf_result[i]['pos'], 'term': crf_result[i]['term'], 'type': 'subject4'})
            # elif crf_tag != 'useless' and weight < 0.2:
            #     new_crf_result.append({'pos': crf_result[i]['pos'], 'term': crf_result[i]['term'], 'type': 'useless'})
            else:
                new_crf_result.append(crf_result[i])
        result = {'data': new_crf_result, 'term_weight': xgboost_result}
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
        tr_res = self.tune_crf_tag(tr_res)

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
            if crf_result[i]['type'] != 'useless' and crf_result[i]['type'] != 'time':
                brief += crf_result[i]['term']
        final_result["brief"] = brief

        # 处理时间
        final_result['years'] = self.te.extract_regex(sentence)
        print(final_result)
        return final_result


# a = Collector()
# a.collect('ofo摩拜新增用户量走势')
