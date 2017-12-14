import gensim
import re
import numpy as np
from jpype import *
import xgboost as xgb
from pyspark import *


class Utility:
    """
    功能性类，提供分词,词向量,xgboost预测功能
    """

    def __init__(self):
        class_path = '/home/abc/soft/hanlp/hanlp-pure-1.3.4.jar:/home/abc/soft/hanlp/'

        word2vec_path = SparkFiles.get('abst_title.model')

        if not isJVMStarted():
            startJVM(getDefaultJVMPath(), "-Djava.class.path=" + class_path, "-Xms1g", "-Xmx1g")
        self.HanLP = JClass('com.hankcs.hanlp.HanLP')
        self.model = gensim.models.Word2Vec.load(word2vec_path)
        self.bst = xgb.Booster()
        self.bst.load_model(SparkFiles.get('time.model'))

    def word_segmentor(self, sentence):
        """
        返回一个句子的 Hanlp 的分词结果
        :param sentence: 
        :return: 
        """
        return [x.word for x in list(self.HanLP.segment(sentence))]

    def get_vector(self, words):
        """
        返回一组词的 Word2Vec 向量
        :param words: 
        :return: 
        """
        sentence_vec = np.zeros(100)
        for i in words:
            if i not in self.model:
                print('"{0}" 不在 Word2Vec 模型里'.format(i))
                vec = np.zeros(100)
            else:
                vec = self.model[i]
            sentence_vec += vec
        norm_sentence_vec = sentence_vec/float(len(words)) if len(words) != 0 else sentence_vec
        return norm_sentence_vec.tolist()

    @staticmethod
    def is_time_sentence(token_sentence):
        """
        判断一个图列是否符合时间序列
        :param token_sentence: 
        :return: 
        """

        def is_date(sentence):
            pattern1 = re.compile(u'(\d*年)(\d*月)?(\d*日)?')
            pattern2 = re.compile(u'(\d*月)(\d*日)?')
            pattern3 = re.compile(u'(\d*日)|(时间)')
            pattern4 = re.compile(u'(\d*周)|(\d*上)|(\d*下)')
            match1 = pattern1.search(sentence)
            match2 = pattern2.search(sentence)
            match3 = pattern3.search(sentence)
            match4 = pattern4.search(sentence)
            if match1 is not None or match2 is not None or match3 is not None or match4 is not None:
                return True
            else:
                return False

        def contain_chinese(sentence):
            zh_pattern = re.compile(u'[\u4e00-\u9fa5]+')
            match = zh_pattern.search(sentence)
            if match is not None:
                return True
            if match is None:
                return False

        if is_date(token_sentence) or not contain_chinese(token_sentence):
            return True
        if not is_date(token_sentence) and contain_chinese(token_sentence):
            return False

    @staticmethod
    def extract_time(title, axis):
        """
        获取时间，如果图片的轴上有多个时间，则排序后返回并忽略标题里的时间；如果图片的轴上没时间，则返回标题里的时间的排序结果
        :param title: 图标题，比如 '11至13年国内主要指数估值图'
        :param axis: 图的下方轴坐标，比如 ['2008-01-04', '2009-03-07', '2010-05-09', '2011-07-11']
        :return: 
        """
        title_years = []
        axis_years = []

        title = title.replace(' ', '')  # 去除空格，比如'2 0 1 7'这样的

        # 时间段，比如 '12-13年'
        pattern1 = re.compile(r'(?<![\d])(20)\d{2}年?[-~到至](20)?\d{2}年?|(?<![\d])\d{2}年?[-~到至]\d{2}年')
        # 单个带年的时间（只考虑2000年-2099年），比如'2012年', '12年'
        pattern2 = re.compile(r'(?<![\d])(20)?\d{2}年')
        # 单个不带年的时间（只考虑2000年-2099年），比如'2012'
        pattern3 = re.compile(r'(?<![\d])(20\d{2})(?![\d])')
        match1 = re.search(pattern1, title)
        match2 = re.search(pattern2, title)
        match3 = re.search(pattern3, title)
        if match1 is not None:
            years = re.split('[-~到至]', match1.group())
            for i in years:
                temp = re.search('[12]\d{3}|\d{2}', i)
                if temp is not None:
                    title_years.append(temp.group())
        if match2 is not None:
            temp = re.search('(20)?\d{2}', match2.group())
            if temp is not None:
                title_years.append(temp.group())
        if match3 is not None:
            title_years.append(match3.group())

        # 匹配年份的，比如'2012年','13年'
        axis_pattern1 = re.compile(r'(?<![\d])(20)?\d{2}年')
        # 按年月日排序的，且中间有'-/.'间隔的时间，比如'2008-1-2','2015/Sept/03','2014/02/12','2012.3.25',
        axis_pattern2 = re.compile(
            '(20\d{2}年?)[/-/.]([01]?\d月?|[a-zA-Z]{3,4})[/-/.]([0123]?\d日?)')
        # 按月日年排序的，且中间有'-/'间隔的时间，比如'1-2-2008','Sept/03/2015','02/12/2014','3.25.2012',
        axis_pattern3 = re.compile(
            '([01]?\d月?|[a-zA-Z]{3,4})[/-/.]([0123]?\d日?)[/-/.](20\d{2}年?)')
        # 匹配带有季度和半年的，比如'2008Q3','2012H2', '12Q4', '2Q15'
        axis_pattern4 = re.compile(r'(?<![\d])((20)?\d{2}[AEHQ])|([AEHQ](20)?\d{2})(?![\d])')
        # 匹配年（>12）月（<=12）的，比如'15/04','13-2'，注意，'10/11'无法判断，因为可能是10月11日
        axis_pattern5 = re.compile(r'(?<![\d/-])(1[3-9]|[2-9]\d)[/-](0?\d|1[012])(?![\d]/-)')
        # 匹配可能是时间的四位数字（其前后都是非数字，以20开头），比如'2008',
        axis_pattern6 = re.compile(r'(?<![\d])(20)\d{2}(?![\d])')
        for i in axis:
            month_mapping = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
                             'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}
            for key in month_mapping:
                pattern = re.compile(key, re.IGNORECASE)
                i = pattern.sub(month_mapping[key], i)
            i = i.replace(' ', '')
            axis_match1 = re.search(axis_pattern1, i)
            axis_match2 = re.search(axis_pattern2, i)
            axis_match3 = re.search(axis_pattern3, i)
            axis_match4 = re.search(axis_pattern4, i)
            axis_match5 = re.search(axis_pattern5, i)
            axis_match6 = re.search(axis_pattern6, i)

            if axis_match1 is not None:
                year = axis_match1.group().strip('年')
                axis_years.append(year)

            elif axis_match2 is not None:
                year = re.split('[/-/.]', axis_match2.group())[0].strip('年').strip()
                axis_years.append(year)

            elif axis_match3 is not None:
                year = re.split('[/-/.]', axis_match3.group())[2].strip('年').strip()
                axis_years.append(year)

            elif axis_match4 is not None:
                year = re.search('20\d{2}|\d{2}', axis_match4.group())
                axis_years.append(year.group())

            elif axis_match5 is not None:
                year = re.split('[/-]', axis_match5.group())[0]
                axis_years.append(year)

            elif axis_match6 is not None:
                year = axis_match6.group()
                axis_years.append(year)

        # 处理混淆的特殊情况，比如 ['01/9/15','07/9/15','01/9/16', '07/9/16']，单独看每个时间我们无法确定是
        # '15年1月9日'还是'01年9月15日'，除了判断月份不能超过12,我们只能通过整体推断最后一位才是年份，判断原则是采取两种理解方式进行解析时间，
        # 解析为递增的则是正确方式（图片的轴上的时间肯定是逐步增大的），如果两种都是递增或都是递减，那则无法判断年份，
        # 比如 ['08/01/15','08/01/16','08/01/17']
        flag = True
        matched_times = []
        year_position = -1
        for i in axis:
            mix_pattern = re.compile(r'(?<![\d])\d{1,2}[/-]\d{1,2}[/-]\d{1,2}(?![\d])')
            has_match = re.search(mix_pattern, i)
            if has_match is None:
                flag = False
                break
            else:
                # 如果第一个位置大于12,则第一个位置是年份
                if int(re.split('[/-]', i)[0]) > 12:
                    year_position = 0
                # 如果第二个位置大于12,则第三个位置是年份
                if int(re.split('[/-]', i)[1]) > 12:
                    year_position = 2
                matched_times.append(has_match.group())
        if flag:
            if year_position == -1:
                times_1 = [
                    int(re.split('[/-]', x)[0]) * 365 + int(re.split('[/-]', x)[1]) * 30 + int(re.split('[/-]', x)[2])
                    for x in matched_times]  # 年份在第一个位置
                times_2 = [
                    int(re.split('[/-]', x)[2]) * 365 + int(re.split('[/-]', x)[0]) * 30 + int(re.split('[/-]', x)[1])
                    for x in matched_times]  # 年份在第三个位置
                is_ascending_1 = sorted(times_1) == times_1
                is_ascending_2 = sorted(times_2) == times_2
                if is_ascending_1 and not is_ascending_2:
                    year_position = 0
                if not is_ascending_1 and is_ascending_2:
                    year_position = 2
            if year_position != -1:
                for j in matched_times:
                    axis_years.append(re.split('[/-]', j)[year_position])

        # 把年份时间处理成统一格式，比如在 8->2008, 22 -> 2022, 01 -> 2001，并且除去里面重复的之后进行排序
        def norm_year(list_years):
            norm_years = []
            for _year in list_years:
                if len(_year) == 1:
                    if int('200' + _year) not in norm_years:
                        norm_years.append(int('200' + _year))
                elif len(_year) == 2:
                    if int('20' + _year) not in norm_years:
                        norm_years.append(int('20' + _year))
                elif int(_year) not in norm_years:
                    norm_years.append(int(_year))
            return [str(x) for x in sorted(norm_years, reverse=True)]

        if axis_years:
            return norm_year(axis_years)
        elif title_years:
            return norm_year(title_years)
        else:
            return []

    def get_probability(self, title, x_axis):
        """
        根据标题和数轴数据中提取出年份
        :param title: 标题
        :param x_axis: 数轴数据
        :return: 
        """

        time_pattern1 = re.compile('(趋势)|(走势)|增速|走勢|趨勢|增长|至今|迄今|历年|个月|以来|逐年|小幅|大幅')
        time_pattern2 = re.compile('([最近])?([近])?([半一二三四五六七八九十两单当上本几])?([年月季周])([内])?')

        # 句子前的无用的多余信息
        redundant_pattern = re.compile(r'(附)?([图])([表])?(\s)?(\d)*([-.])*(\d)*(\.)?(\d)*([.：:、\s])*')

        match1 = time_pattern1.search(title)
        match2 = time_pattern2.search(title)

        axis = ','.join(x_axis)

        if match1 is not None or match2 is not None:
            return float(1)
        elif axis:
            if self.is_time_sentence(axis):
                return float(1)
            else:
                return float(0)
        else:
            redundant = re.match(redundant_pattern, title)
            striped_title = ''

            if redundant is not None:
                striped_title = re.split(redundant.group(), title)[-1]

            words = self.word_segmentor(striped_title)
            input_matrix = np.array([self.get_vector(words)])
            dtest = xgb.DMatrix(input_matrix)
            pre_result = self.bst.predict(dtest)
            probability = float(pre_result[0][1])
            return probability


# Test
# if __name__ == "__main__":
#     u = Utility()
#     print(u.extract_time('2015-2017年内走势', ['12/03', '13/03', '14/03']))
#     print(u.get_probability('2015-2017年内走势', ['12/03', '13/03', '14/03']))
#     print(u.get_vector(['石油']))
#     print(u.is_time_sentence('2017年内走势'))
#     print(u.word_segmentor('全球石油期货情况'))
