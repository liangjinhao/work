import re
import hashlib
import hanlp_segmentor
import threading


class Singleton(type):

    _instances = dict()
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):

        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        else:
            with cls._lock:
                if cls in cls._instances:
                    cls._instances[cls].__init__(*args, **kwargs)

        return cls._instances[cls]


class Hash(metaclass=Singleton):
    """
    根据一篇网页的内容和标题计算出一个表征该网页内容的特征值
    """

    def __init__(self):
        # get idf dict
        self.dit = {}
        with open("./idf_clean.txt", "r") as f:
            for line in f:
                line = line.strip("\n").split(",")
                k = line[0]
                v = float(line[1])
                if k not in self.dit:
                    self.dit[k] = v
                else:
                    pass
        f.close()
        self.sgementor = hanlp_segmentor.HanlpSegmentor()

    def clean_title(self, title):
        """
        清洗掉 Title 中的时间
        :param title: 网页文章标题
        :return: 清洗过的标题
        """
        rr1 = re.compile(r'([0-9]*)\.([0-9+]*)')
        rr2 = re.compile(r'([0-9]*)月|([0-9]*)日')
        clean_title = ''
        if rr1.findall(title):
            res1 = ["".join(ks) for ks in rr1.findall(title)]
            clean_title = "".join(res1)
        elif rr2.findall(title):
            res2 = ["".join(ks) for ks in rr2.findall(title)]
            clean_title = "".join(res2)
        return clean_title

    def cut_content(self, content):
        """
        从网页内容（源网页带标签的文本）中选取出6个 TF-IDF 最高的词
        :param content: 源网页带标签的文本
        :return: 6个 TF-IDF 最高的词 list
        """
        # 按网页标签将网页内容切成各个段落
        unlabel_content = re.split(
            "\\n|\\r|/address|/caption|/dd|/div|/dl|/dt|/fieldset|/form|/h1|/h2|/h3|/h4|/h5|/h6|/hr|/legend|/li|"
            "/noframes|/noscript|/ol|/ul|/p|/pre|/table|/tbody|/td|/tfood|/th|/thead|/tr|br/?", content)
        clean_content = map(lambda x: "".join(re.findall('[\u4e00-\u9fa5]+', x)), unlabel_content)
        # 选取网页中最长的3个段落
        clean_content_sorted = sorted(clean_content, key=lambda x: len(x), reverse=True)[:3]
        # 分词统计TF
        content_dit = {}
        segs = self.sgementor.get_segments(clean_content_sorted)
        for i in segs:
            term = i[0]
            if term.word not in content_dit:
                content_dit[term.word] = 1
            else:
                content_dit[term.word] += 1
        # TF-IDF 提取权重最高的 6 个词
        keyWords = []
        for word in content_dit:
            if word in self.dit:
                tf_idf = [word, self.dit[word]*content_dit[word]]
                keyWords.append(tf_idf)
            else:
                keyWords.append([word, -100])
        keyWord = sorted(keyWords, key=lambda x: x[1], reverse=True)
        top_words = list(map(lambda x: x[0], keyWord[:6]))
        return top_words

    def get_hash(self, title, content):
        """
        获取网页的 hash 特征值
        :param title: 网页标题
        :param content: 待标签的网页内容
        :return:
        """
        content_ = self.cut_content(content)
        title_ = self.clean_title(title)
        if title_:
            title_content = title_+"".join(content_)
            value = int(hashlib.md5(bytes(title_content, 'utf-8')).hexdigest(), 16)
        else:
            value = int(hashlib.md5(bytes("".join(content_), 'utf-8')).hexdigest(), 16)
        return [value, content_]
