import re
import datetime


class TimeExtractor:
    """
    从一个句子中抽取出年份信息
    """

    def __init__(self):

        self.rules = {
            # 2015-2017，13-17年
            '1': '(?<![\d])(?:20)\d{2}年?[-~到至](?:20)?\d{2}年?|(?<![\d])\d{2}年?[-~到至]\d{2}年',
            # 2017，17年，
            '2': '(?<![\d])20\d{2}(?![\d])|(?<![\d])\d{2}年',
            # '20170101','2017-01-01','2017/01/01','2017.01.01'
            '3': '(20\d{2}[-/.](?:1[0-2]|0?[1-9])[-/.](?:[12][0-9]|3[01]|0?[1-9])|'
                 '(?<![\d])20\d{2}(?:1[0-2]|0?[1-9])(?:[12][0-9]|3[01]|0?[1-9])(?![\d]))',
            # '一七年',
            '4': '(?:[零〇一二])?[零〇一二三四五六七八九]{2}年',
            # '前年','去年','今年','明年'
            '5': '[前去今明后]年',
            # '前两年','两年前','两年来','后三年','近10年',
            '6': '((?:[前近]|至今|过去)(?:[一二三四五六七八九]{1,2}|[1-9]|[12][0-9])年|'
                 '(?:[一二三四五六七八九]{1,2}|[1-9]|[12][0-9])年(?:[内来]|以来))',
        }

        self.number_mapping = {
            '〇': '0', '零': '0', '一': '1', '二': '2', '三': '3', '四': '4',
            '五': '5', '六': '6', '七': '7', '八': '8', '九': '9'
        }

    def extract(self, sentence):

        years = []
        sentence = sentence.replace(' ', '')

        for rule_key in self.rules:

            if rule_key == '1':
                match = re.findall(self.rules[rule_key], sentence)
                if match:
                    for raw_time in match:
                        years_range = re.split('[-~到至]', raw_time)
                        start_year_raw = years_range[0].replace('年', '')
                        end_year_raw = years_range[1].replace('年', '')
                        start_year = start_year_raw if len(start_year_raw) == 4 else '20' + start_year_raw
                        end_year = end_year_raw if len(end_year_raw) == 4 else '20' + end_year_raw
                        interval_years = [x for x in range(eval(start_year), eval(end_year)+1)]
                        years.extend([str(x) for x in interval_years if str(x) not in years])

            if rule_key == '2':
                match = re.findall(self.rules[rule_key], sentence)
                if match:
                    for raw_time in match:
                        norm_time = raw_time if len(raw_time) == 4 else '20' + raw_time.strip('年')
                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '3':
                match = re.findall(self.rules[rule_key], sentence)
                if match:
                    for raw_time in match:
                        norm_time = raw_time[:4]
                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '4':
                match = re.findall(self.rules[rule_key], sentence)
                if match:
                    for raw_time in match:
                        norm_time = raw_time.strip('年')
                        for i in norm_time:
                            norm_time = norm_time.replace(i, self.number_mapping[i])
                        norm_time = norm_time if len(norm_time) == 4 else '20' + norm_time
                        if norm_time not in years:
                            years.append(norm_time)

            if rule_key == '5':
                match = re.findall(self.rules[rule_key], sentence)
                if match:
                    current_year = datetime.date.today().year
                    for raw_time in match:
                        if raw_time == '前年':
                            years.append(str(current_year-2))
                        elif raw_time == '去年':
                            years.append(str(current_year-1))
                        elif raw_time == '今年':
                            years.append(str(current_year))
                        elif raw_time == '明年':
                            years.append(str(current_year+1))
                        elif raw_time == '后年':
                            years.append(str(current_year+2))

            if rule_key == '6':
                match = re.findall(self.rules[rule_key], sentence)
                if match:
                    current_year = datetime.date.today().year
                    for raw_time in match:
                        interval = raw_time
                        for i in raw_time:
                            if i in self.number_mapping:
                                interval = interval.replace(i, self.number_mapping[i])
                        interval = re.search('\d{1,2}', interval).group()
                        interval_years = [x for x in range(current_year - int(interval) + 1, current_year + 1)]
                        years.extend([str(x) for x in interval_years if str(x) not in years])
        
        years.sort()
        return years


# a = TimeExtractor()
# print(a.extract('打法fd22015-2017年adf218-19年dsfa'))
# print(a.extract('打1234年法2045年daf3423年'))
# print(a.extract('打20071113近2年年'))
# print(a.extract('打法一九年年'))
# print(a.extract('2去年和后年的'))
# print(a.extract('fs过去3年发散'))
