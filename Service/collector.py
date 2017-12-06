import os
import inspect
import re
import configparser
import hanlp_segmentor
import term_ranking
import ac_search

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
            "term_weight": []  # 句子中各个词语的ranking
        })
        final_result["title"] = sentence

        # 处理term_weight
        tr_res = self.term_rank.predict_query(sentence)
        final_result["term_weight"] = tr_res["term_weight"]

        # 处理data
        crf_result = tr_res["data"]
        for i in range(len(crf_result)):
            var = crf_result[i]
            # 英文的识别很差，先统一看成主体
            if re.match(r'[a-zA-Z]+', var['term']):
                final_result["data"].append({'term': var['term'], 'type': 'subject'})
            else:
                final_result["data"].append({'term': var['term'], 'type': var['type']})

        # 处理brief
        brief = sentence
        match = self.ahocorasick.search(sentence)
        for i in range(len(match)):
            word = match[i]
            # 如果是英文，则不是字符匹配，要考虑英文中的空格（比如，如果字典里有'IT'，如果对英文进行直接的字符匹配，会把'MIT'这种词给处理掉）
            res = re.match(u'([a-zA-Z-/\\\]+[ ]*)*[a-zA-Z-/\\\]+', word)
            if res:
                if not re.search(u'(^|[^A-Za-z])' + res.group(0) + u'($|[^A-Za-z])', sentence):
                    continue
            brief = brief.replace(word, '')

        for i in crf_result:
            if i['type'] == 'useless':
                brief = brief.replace(i['term'], '')
            if i['type'] == 'time':
                brief = brief.replace(i['term'], '')
            if i['type'] == 'time':
                brief = brief.replace(i['term'], '')

        final_result["brief"] = brief

        return final_result

# a = Collector()
# print(a.collect('图表13:过去三年平安银行浦发银行国内生产总值情况'))
