import os
import inspect
import configparser
import re
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
        self.predict_file_path = self.test_file_path + '_predict'

        self.segmentor = hanlp_segmentor.HanlpSegmentor()

    @staticmethod
    def exam_crf_train_file(file_path):
        """
        检查CRF训练文件是否符合规范
        :param file_path:
        :return:
        """

        # 每行的格式
        pattern_string = u'^[\S]{1,30}\t([\d]|[a-z]|(10))$'
        pattern = re.compile(pattern_string)

        i = 0
        is_wrong = False
        message = ''
        with open(file_path, 'rU') as f:
            for line in f:
                i = i + 1
                if line == '\n' or line == '\r\n':
                    continue
                if re.match(pattern, line):
                    continue
                else:
                    is_wrong = True
                    message += 'Line:' + str(i) + '\t' + line
        if is_wrong:
            print('文件：' + file_path + '存在格式错误,请查看：' + file_path + '_格式检测结果')
            f_write = open(file_path + '_格式检测结果', 'w')
            f_write.write(message)
            f_write.close()

    def train(self):
        """
        训练CRF模型
        :return:
        """
        # param:-m iter次数;-f使用的特征出现次数不能少于多少,默认是1;-c控制拟合，这个值比较大的时候容易过拟合,默认是1.
        # 当上面不设置m参数的时候，模型会等待console输出的obj收敛于某个值的时候停止训练.
        cmd_command = 'crf_learn' + ' ' + self.template_file_path + ' ' + self.train_file_path + ' ' + \
                      self.model_file_path + ' -t'
        os.system(cmd_command)

    def predict_file(self):
        """
        预测一个文件的标注结果，该文件的格式必须满足CRF++规定的格式
        :return:
        """
        cmd_command = 'crf_test' + ' -m ' + self.model_file_path + ' ' + self.test_file_path + '>>' + \
                      self.predict_file_path
        os.system(cmd_command)

    def extract(self, sentence):
        """
        返回一个句子的CRF标注结果
        :param sentence: 输入的句子
        :return: 句子分词后各个词的词性，CRF标注结果
        """

        mappings = {'1': 'subject1', '2': 'subject2', '3': 'subject3', '4': 'indicator', '5': 'product', '6': 'area',
                    '7': 'formula', '8': 'useless', '9': 'time'}

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

        # 把空格替换成，（因为CRF++忽略空格而分词不忽略，导致对齐出错）
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


# a = CRF()
# print(a.extract('去年万科每月收入'))
