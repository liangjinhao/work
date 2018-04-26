import sys
import grpc
import hanlp_pb2
import hanlp_pb2_grpc
import re
import hashlib


class Hash():
    """
    Used to get the hash value of the title and content
    """
    def __init__(self):
        # get idf dit
        self.dit = {}
        with open("../idf_clean.txt", "r") as f:
            for line in f:
                line = line.strip("\n").split(",")
                k = line[0]
                v = float(line[1])
                if k not in self.dit:
                    self.dit[k] = v
                else:
                    pass
        f.close()
        # initialize connection
        _HOST = '121.40.125.154'
        _PORT = '50051'
        conn = grpc.insecure_channel(_HOST + ':' + _PORT)
        self.client = hanlp_pb2_grpc.GreeterStub(channel=conn)

    # 东风分词接口, 对content进行分词
    def run(self, texts):
        # 分词，返回结果
        response = self.client.segment(
            hanlp_pb2.HanlpRequest(text=texts, indexMode=0, nameRecognize=1, translatedNameRecognize=1))
        # TF词频
        dit = {}
        # return response.data
        for term in response.data:
            if term.word != " " and term.word != ",":
                # print term.word, len(term.word)
                if term.word not in dit:
                    dit[term.word] = 1
                else:
                    dit[term.word] += 1
        return dit

    # title处理
    def clean_title(self, title):
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

    # content切词 并 依据TF-IDF抽取关键词
    def cut_content(self, content):
        # 东风分词接口
        # content = "".join(re.findall(ur"[\u4e00-\u9fa5]+", content, re.S))
        content_dit = self.run(content)
        # TF-IDF提取权重最高的８个词
        keyWords = []
        for word in content_dit:
            if word in self.dit:
                tf_idf = [word, self.dit[word]*content_dit[word]]
                keyWords.append(tf_idf)
            else:
                keyWords.append([word, -100])
        keyWord = sorted(keyWords, key=lambda x: x[1], reverse=True)
        # 取前８个
        keyWord = map(lambda x: x[0], keyWord[:8])
        return keyWord

    # hash映射函数
    def hash_func(self, x):
        return int(hashlib.md5(x).hexdigest(), 16)

    # 获取该文章的hash值
    def get_hash(self, title, content):
        content_ = self.cut_content(content)
        title_ = self.clean_title(title)
        # title+content的hash值
        if title_:
            title_content = title_+"".join(content_)
            value = self.hash_func(title_content)
        else:
            value = self.hash_func("".join(content_))
        # return value
        return [value, content_]
