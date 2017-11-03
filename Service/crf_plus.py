import os
import inspect
import configparser
import CRFPP
import hanlp_segmentor

CONFIG_FILE = "path.conf"


class CRF:
    """
    CRF模型的训练、预测
    """

    def __init__(self):
        home_dir = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)

        # 各种路径
        self.model_file_path = home_dir + conf.get("crf", "model")
        self.template_file_path = home_dir + conf.get("crf", "template")

        self.train_file_path = home_dir + conf.get("crf", "train_file")
        self.test_file_path = home_dir + conf.get("crf", "test_file")
        self.predict_file_path = self.test_file_path + '_预测结果'

        self.segmentor = hanlp_segmentor.HanlpSegmentor()

    def train(self):
        cmd_command = 'crf_learn' + ' ' + self.template_file_path + ' ' + self.train_file_path + ' ' + \
                      self.model_file_path + ' -t'
        os.system(cmd_command)

    def extract(self, sentence):

        mappings = {'1': 'subject', '2': 'indicator', '3': 'time', '4': 'area', '5': 'formula', '6': 'frequency',
                    '7': 'conjunction', 'a': 'main', 'b': 'minor', 'c': 'description', 'd': 'difinitive',
                    'o': 'useless'}

        # mappings = {1: 'subject', 2: 'industry', 3: 'stock_block', 4: 'indicator', 5: 'product', 6: 'area',
        #             7: 'formula', 8: 'main', 9: 'minor', 10: 'time', 0: 'useless'}

        tagger = CRFPP.Tagger("-m " + self.model_file_path)

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

        for i in range(len(chars)):
            tagger.add((new_chars[i] + '\t' + char_pos[i]))

        tagger.parse()
        size = tagger.size()  # token size
        xsize = tagger.xsize()  # column size
        for i in range(0, size):
            char = tagger.x(i, 0)
            tag = tagger.y2(i)
            char_tag.append(tag)

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

    def predict_file(self):
        cmd_command = 'crf_test' + ' -m ' + self.model_file_path + ' ' + self.train_file_path + '>>' + \
                      self.predict_file_path
        os.system(cmd_command)

# a = CRF()
# print(a.extract('去年万科每月收入'))
