import configparser
from jpype import *

CONFIG_FILE = "path.conf"


class HanlpSegmentor:

    def __init__(self):
        """
        启动JVM生产Hanlp实例
        """
        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)
        class_path = conf.get("hanlp", "classpath")
        if not isJVMStarted():
            startJVM(getDefaultJVMPath(), "-Djava.class.path=" + class_path, "-Xms1g", "-Xmx1g")  # 启动JVM
        self.HanLP = JClass('com.hankcs.hanlp.HanLP')

    def get_segments(self, sentence):
        """
        对一个句子进行分词
        :param sentence: 输入的句子
        :return: 分词的结果（词和词性组成的元组的列表）
        """
        res = list(self.HanLP.segment(sentence))
        result = []
        for item in res:
            word = item.word
            nature = java.lang.String.valueOf(item.nature)
            result.append((word, nature))
        return result

# a = HanlpSegmentor()
# print(a.get_segments(u'上实发展资产负债表'))
