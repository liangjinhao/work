import sys
import configparser
import hanlp_segmentor

CONFIG_FILE = "path.conf"

conf = configparser.ConfigParser()
conf.read(CONFIG_FILE)
bilstm_crf_path_path = conf.get("tensorflow_crf", "bilstm_crf_path")
sys.path.append(bilstm_crf_path_path)
from crf_lstm_model import *


class CRF:

    def __init__(self):
        self.get_tag_obj = SequenceTag()
        self.segmentor = hanlp_segmentor.HanlpSegmentor()

    def extract(self, sentence):
        """
        从一个句子中抽取出主体/指标/时间等信息
        :param sentence: 一个句子，字符串
        :return: 返回抽取的结果，是个字典
        """
        mappings = {1: 'subject', 2: 'industry', 3: 'stock_block', 4: 'indicator', 5: 'product', 6: 'area',
                    7: 'formula', 8: 'main', 9: 'minor', 10: 'time', 0: 'useless'}

        # 把空格替换成逗号
        chars = []  # 一句话里的字符
        char_pos = []  # 一句话里的字符的词性
        char_tag = []  # 一句话里的字符的crf预测标签
        words = []  # 一句话里的词
        pos = []  # 一句话里的词的词性
        word_tag = []  # 一句话里的词的crf预测标签

        res = self.segmentor.get_segments(sentence)
        for item in res:
            chars.extend(list(item[0]))
            char_pos.extend([item[1]] * len(item[0]))
            words.append(item[0])
            pos.append(item[1])

        new_chars = []
        replaced_char = ','
        index = []
        for i in range(len(chars)):
            if chars[i] == ' ':
                index.append(i)
                new_chars.append(replaced_char)
            else:
                new_chars.append(chars[i])

        # 获取char_tag
        char_tag = self.get_tag_obj.get_tag(sentence)

        # 获取word_tag
        start_pos = 0
        for i in words:
            end_pos = start_pos + len(i)
            tag_num = {}
            for j in range(start_pos, end_pos):
                tag_num[char_tag[j]] = tag_num[char_tag[j]] + 1 if char_tag[j] in tag_num else 1
            start_pos = end_pos
            tag = sorted(tag_num.items(), key=lambda d: d[1], reverse=True)[0][0]
            word_tag.append(tag)

        result = []
        for i in range(len(words)):
            data = dict()
            data['term'] = words[i]
            data['type'] = mappings[word_tag[i]]
            data['pos'] = pos[i]
            result.append(data)

        return result


# a = CRFExtract()
# print(a.extract('万科收入'))
# [{'term': '万科', 'type': 'subject', 'pos': 'nz'}, {'term': '收入', 'type': 'indicator', 'pos': 'n'}]
