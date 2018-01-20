import re
import datetime


class ABCYear:

    @staticmethod
    def extract(sentence, axis_info, current_year=None):

        # 如果没有指定当前年份，则默认是此时刻的所在年份
        if not current_year:
            current_year = datetime.date.today().year

        x = ABCYear.extract_regex(sentence, current_year)
        y = ABCYear.extract_axis(axis_info, current_year)
        [x.append(i) for i in y if i not in x]
        x.sort()
        return x

    @staticmethod
    def extract_regex(sentence, current_year=None):
        """
        通过正则表达式方式抽取出一个句子里的年份信息
        :param sentence: 输入的句子
        :param current_year: 当前年份，整数
        :return: 从最近到最远排列的抽取出的时间
        """

        # 如果没有指定当前年份，则默认是此时刻的所在年份
        if not current_year:
            current_year = datetime.date.today().year

        regexes = {
            # 年份时间段：比如 '2015-2017'，'93-17年'
            '1': '(?<![\d])(?:19|20)\d{2}年?[-~到至](?:19|20)?\d{2}年?|(?<![\d])\d{2}年?[-~到至]\d{2}年',
            # 年份：比如 '2017'，'17年'，'97年'
            '2': '(?<![\d])(?:19|20)\d{2}(?![\d])|(?<![\d])\d{2}年',
            # 特殊简称：比如 '17Q3'，'2Q16'，'15H2'，'FY15'，'H1'14'，'Q3 17'
            '3': "(?<![\d])\d{2}[AEHQ][1-4]|"
                 "(?<![\d])\d{2}[' ][1-4][AEHQ]|"
                 "[1-4][AHQ]\d{2}(?![\d])|"
                 "[AHQ][1-4][' ]\d{2}(?![\d])|"
                 "(?<![\d])\d{2}FY|"
                 "FY\d{2}(?![\d])",
            # 详细时间：比如 '20170101'，'2017-01-01'，'2017/01/01'，'2017.01.01'
            '4': '((?:19|20)\d{2}[-/.](?:1[0-2]|0?[1-9])[-/.](?:[12][0-9]|3[01]|0?[1-9])|'
                 '(?<![\d])(?:19|20)\d{2}(?:1[0-2]|0?[1-9])(?:[12][0-9]|3[01]|0?[1-9])(?![\d]))',
            # 汉字年份：比如 '二〇一七年'
            '5': '(?:二[零〇])?[零〇一二三四五六七八九]{2}年',
            # 带月份的时间
            '6': '(?:JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)[- ]\d{2}',
            # 其他表述：比如 '前两年'，'近两年'，'两年来'，'三年以来'，'近10年'
            '7': '((?:[前近]|至今|过去)(?:[一二三四五六七八九两]|[1-9]|[12][0-9])年|'
                 '(?:[一二三四五六七八九两]|[1-9]|[12][0-9])年(?:[内来]|以来))'
        }

        number_mapping = {
            '〇': '0', '零': '0', '一': '1', '二': '2', '三': '3', '四': '4',
            '五': '5', '六': '6', '七': '7', '八': '8', '九': '9', '两': '2'
        }

        years = []
        sentence = sentence.replace(' ', '')

        for rule_key in regexes:

            if rule_key == '1':
                match = re.findall(regexes[rule_key], sentence)
                if match:
                    for raw_time in match:
                        years_range = re.split('[-~到至]', raw_time)
                        start_year_raw = years_range[0].replace('年', '')
                        end_year_raw = years_range[1].replace('年', '')

                        if len(start_year_raw) == 4:
                            start_year = start_year_raw
                        else:
                            # 只考虑上世纪80年代之后的
                            if start_year_raw > '80':
                                start_year = '19' + start_year_raw
                            else:
                                start_year = '20' + start_year_raw

                        if len(end_year_raw) == 4:
                            end_year = end_year_raw
                        else:
                            # 只考虑上世纪80年代之后的
                            if end_year_raw > '80':
                                end_year = '19' + end_year_raw
                            else:
                                end_year = '20' + end_year_raw

                        interval_years = [x for x in range(eval(start_year), eval(end_year) + 1)]
                        years.extend([str(x) for x in interval_years if str(x) not in years])

            if rule_key == '2':
                match = re.findall(regexes[rule_key], sentence)
                if match:
                    for raw_time in match:

                        if len(raw_time) == 4:
                            norm_time = raw_time
                        else:
                            # 只考虑上世纪80年代之后的
                            if raw_time.strip('年') > '80':
                                norm_time = '19' + raw_time.strip('年')
                            else:
                                norm_time = '20' + raw_time.strip('年')

                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '3':
                match = re.findall(regexes[rule_key], sentence)
                if match:
                    for raw_time in match:
                        year = re.search('\d{2}', raw_time).group()
                        if year > '80':
                            norm_time = '19' + year
                        else:
                            norm_time = '20' + year
                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '4':
                match = re.findall(regexes[rule_key], sentence)
                if match:
                    for raw_time in match:
                        norm_time = raw_time[:4]
                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '5':
                match = re.findall(regexes[rule_key], sentence)
                if match:
                    for raw_time in match:
                        norm_time = raw_time.strip('年')
                        for i in norm_time:
                            norm_time = norm_time.replace(i, number_mapping[i])
                        norm_time = norm_time if len(norm_time) == 4 else '20' + norm_time
                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '6':
                match = re.findall(regexes[rule_key], sentence, flags=re.IGNORECASE)
                if match:
                    for raw_time in match:
                        year = re.search('\d{2}', raw_time).group()
                        if year > '80':
                            norm_time = '19' + year
                        else:
                            norm_time = '20' + year
                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '7':
                match = re.findall(regexes[rule_key], sentence)
                if match:
                    for raw_time in match:
                        interval = raw_time
                        for i in raw_time:
                            if i in number_mapping:
                                interval = interval.replace(i, number_mapping[i])
                        interval = re.search('\d{1,2}', interval).group()
                        interval_years = [x for x in range(current_year - int(interval) + 1, current_year + 1)]
                        years.extend([str(x) for x in interval_years if str(x) not in years])

        [years.append(i) for i in ABCYear.extract_rules(sentence, current_year) if i not in years]
        years.sort()
        return years

    @staticmethod
    def extract_rules(sentence, current_year=None):
        """
        将一个句子里的时间短语按规则映射成年份，比如，'最近'被转换成最近一年
        :param sentence:
        :param current_year: 当前年份，整数
        :return:
        """

        # 如果没有指定当前年份，则默认是此时刻的所在年份
        if not current_year:
            current_year = datetime.date.today().year

        mappings = {
            '最近': 0, '近来': 0, '近期': 0, '今年': 0,
            '前年': -2, '去年': -1, '明年': 1,
        }

        years = []
        for key in mappings:
            if key in sentence:
                t = current_year + mappings[key]
                if t not in years:
                    years.append(str(t))

        return years

    @staticmethod
    def extract_axis(axis_info, current_year=None):
        """
        抽取出数轴上面的年份信息
        :param axis_info: 数轴上面的信息，比如 ['2008-01-04', '2009-03-07', '2010-05-09', '2011-07-11']
        :param current_year: 当前年份，整数
        :return:
        """

        # 处理混淆的特殊情况，比如 ['01/9/15','07/9/15','01/9/16', '07/9/16']，单独看每个时间我们无法确定是
        # '15年1月9日'还是'01年9月15日'，除了判断月份不能超过12,我们只能通过整体推断最后一位才是年份，判断原则是采取两种理解方式进行解析时间，
        # 解析为递增的则是正确方式（图片的轴上的时间肯定是逐步增大的），如果两种都是递增，那则无法判断年份，
        # 比如 ['08/01/15','08/01/16','08/01/17']
        years = []

        pattern1_times = []
        pattern1_year_pos = {'is_time': True, 'must': None, 'must_not': None}
        pattern2_times = []
        pattern2_year_pos = {'is_time': True, 'must': None, 'must_not': None}

        for i in axis_info:
            i = i.replace(' ', '')

            [years.append(x) for x in ABCYear.extract_regex(i, current_year) if x not in years]

            month_mapping = {'JAN': '01', 'FEB': '02', 'MAR': '03', 'APR': '04', 'MAY': '05', 'JUN': '06',
                             'JUL': '07', 'AUG': '08', 'SEP': '09', 'OCT': '10', 'NOV': '11', 'DEC': '12'}

            for key in month_mapping:
                pattern = re.compile(key, re.IGNORECASE)
                i = pattern.sub(month_mapping[key], i)

            mix_pattern1 = re.compile(r'(?<![\d])\d{1,2}[/-]\d{1,2}[/-]\d{1,2}(?![\d])')
            match1 = re.findall(mix_pattern1, i)
            mix_pattern2 = re.compile(r'(?<![\d])\d{1,2}[/-]\d{1,2}(?![\d])')
            match2 = re.findall(mix_pattern2, i)

            if match1:
                for j in match1:
                    pattern1_times.append(j)
                    # 如果第一个位置大于12,则第一个位置是年份
                    if int(re.split('[/-]', j)[0]) > 12:
                        if pattern1_year_pos['is_time'] and pattern1_year_pos['must'] != 2:
                            pattern1_year_pos['must'] = 0
                        else:
                            pattern1_year_pos['is_time'] = False
                    # 如果第二个位置大于12,则第三个位置是年份
                    if int(re.split('[/-]', j)[1]) > 12:
                        if pattern1_year_pos['is_time'] and pattern1_year_pos['must'] != 0:
                            pattern1_year_pos['must'] = 2
                        else:
                            pattern1_year_pos['is_time'] = False

            if match2:
                for j in match2:
                    pattern2_times.append(j)

                    # 如果第0个数字大于31,则第0个数字是年份
                    if int(re.split('[/-]', j)[0]) > 31:
                        if pattern2_year_pos['is_time'] and pattern2_year_pos['must_not'] != 0 and \
                                pattern2_year_pos['must'] != 1:
                            pattern2_year_pos['must'] = 0
                            pattern2_year_pos['must_not'] = 1
                        else:
                            pattern2_year_pos['is_time'] = False

                    # 如果第0个数字 12< <31,则第1个数字不可能是年份
                    if 12 < int(re.split('[/-]', j)[0]) <= 31:
                        if pattern2_year_pos['is_time'] and pattern2_year_pos['must'] != 1:
                            pattern2_year_pos['must_not'] = 1
                        if pattern2_year_pos == 1:
                            pattern2_year_pos['is_time'] = False

                    # 如果第1个数字大于31,则第1个数字是年份
                    if int(re.split('[/-]', j)[1]) > 31:

                        if pattern2_year_pos['is_time'] and pattern2_year_pos['must_not'] != 1 and \
                                pattern2_year_pos['must'] != 0:
                            pattern2_year_pos['must'] = 1
                            pattern2_year_pos['must_not'] = 0
                        else:
                            pattern2_year_pos['is_time'] = False

                    # 如果第1个数字 12 < < 31, 则第0个数字不可能是年份
                    if 12 < int(re.split('[/-]', j)[1]) <= 31:
                        if pattern2_year_pos['is_time'] and pattern2_year_pos['must'] != 0:
                            pattern2_year_pos['must_not'] = 0
                        if pattern2_year_pos == 1:
                            pattern2_year_pos['is_time'] = False

        if pattern1_year_pos['is_time']:
            if pattern1_year_pos['must'] is None:

                position_0_list = [int(re.split('[/-]', x)[0]) for x in pattern1_times]
                position_1_list = [int(re.split('[/-]', x)[1]) for x in pattern1_times]
                position_2_list = [int(re.split('[/-]', x)[2]) for x in pattern1_times]

                is_asc_0 = all(position_0_list[i] < position_0_list[i + 1] for i in range(len(position_0_list) - 1))
                is_asc_1 = all(position_1_list[i] < position_1_list[i + 1] for i in range(len(position_1_list) - 1))
                is_asc_2 = all(position_2_list[i] < position_2_list[i + 1] for i in range(len(position_2_list) - 1))

                if is_asc_0 and not is_asc_1 and not is_asc_2:
                    pattern1_year_pos['must'] = 0
                if is_asc_2 and not is_asc_0 and not is_asc_1:
                    pattern1_year_pos['must'] = 2

            if pattern1_year_pos['must'] is not None:
                for j in pattern1_times:
                    year = '20' + re.split('[/-]', j)[pattern1_year_pos['must']]
                    if year not in years:
                        years.append(year)

        if pattern2_year_pos['is_time']:
            position_0_list = [int(re.split('[/-]', x)[0]) for x in pattern2_times]
            position_1_list = [int(re.split('[/-]', x)[1]) for x in pattern2_times]
            is_asc_0 = all(position_0_list[i] <= position_0_list[i + 1] for i in range(len(position_0_list) - 1))
            is_asc_1 = all(position_1_list[i] <= position_1_list[i + 1] for i in range(len(position_1_list) - 1))

            if pattern2_year_pos['must'] is None and pattern2_year_pos['must_not'] == 0 and is_asc_1 and not is_asc_0:
                pattern2_year_pos['must'] = 1
            if pattern2_year_pos['must'] is None and pattern2_year_pos['must_not'] == 1 and is_asc_0 and not is_asc_1:
                pattern2_year_pos['must'] = 0

            if pattern2_year_pos['must'] is not None:
                for j in pattern2_times:
                    year = '20' + re.split('[/-]', j)[pattern2_year_pos['must']]
                    if year not in years:
                        years.append(year)

        years.sort()
        return years


# print(ABCYear.extract('三年', ['Jan-06', 'May-06', 'Sep-06', 'Jan-07', 'May-07', 'Sep-07', 'Jan-08', 'May-08', 'Sep-08', 'Jan-09', 'May-09', 'Sep-09', 'Jan-10', 'May-10', 'Sep-10', 'Jan-11', 'May-11', 'Sep-11', 'Jan-12', 'May-12', 'Sep-12', 'Jan-13']))

