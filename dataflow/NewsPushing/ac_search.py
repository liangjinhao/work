import ahocorasick


class ACSearch:

    def __init__(self):
        """
        初始化AC自动机
        """
        self.ac_automaton = ahocorasick.Automaton()
        self.index = 0

    def add_dict(self, dict_path):
        """
        向Aho-Corasick自动机里添加字典里的词，字典的格式为每行有有多个词，多个词用空格隔开
        :param dict_path: 字典的路径
        :return: 
        """
        with open(dict_path, 'rU') as f:
            for line in f:
                words = line.strip().split()
                for word in words:
                    self.ac_automaton.add_word(word, (self.index, word))
                    self.index += 1

    def add_word(self, word):
        """
        向Aho-Corasick自动机里添加一个词
        :param word: 添入的词
        :return: 
        """
        if type(word) == str:
            self.ac_automaton.add_word(word, (self.index, word))
            self.index += 1

    def start(self):
        """
        加载字典后，调用该函数启动AC自动机
        :return: 
        """
        self.ac_automaton.make_automaton()

    def search(self, string):
        """
        搜索一个字符串里所有的和字典中的词相配的字符串，最长匹配
        :param string: 目标字符串
        :return: 匹配到的字符串list
        """
        if self.ac_automaton.get_stats()['words_count'] == 0:
            return []

        all_items = []
        result = []
        for end_index, (insert_order, value) in self.ac_automaton.iter(string):
            item = [value, end_index+1-len(value), end_index]
            all_items.append(item)

        # 最长匹配，比如对于字符串‘abcbca’，如果字典里有'ab'和'abc'，则匹配返回'abc'，因为'ab'在'abc'中
        for i in all_items:
            for j in all_items:
                if i != j:
                    set1 = set(range(i[1], i[2]))
                    set2 = set(range(j[1], j[2]))
                    if set1 | set2 == set1:
                        if j in all_items:
                            all_items.remove(j)
                    elif set1 | set2 == set2:
                        if i in all_items:
                            all_items.remove(i)
        for i in all_items:
            result.append(i[0])
        return result
