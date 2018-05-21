import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import pymongo
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import COMMASPACE, formatdate
from email import encoders
import os


# 国内 MongoDB 连接信息
MONGODB_HOST = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'search'
PASSWORD = 'ba3Re3ame+Wa'


class MyEmail:
    """
    Use SMTP send emails
    """

    def __init__(self, smtp_server, fromaddr, toaddr, user, password):
        """
        :param smtp_server: smtp server, like 'smtp.gmail.com:587', 'smtp.163.com:25'
        :param fromaddr: your address
        :param toaddr: email addresses you send to, like ['Mike@gmail.com', 'Lucy@gmail.com']
        :param password: your address password
        """
        self.fromaddr = fromaddr
        self.toaddr = toaddr
        self.server = smtplib.SMTP(smtp_server.split(':')[0], int(smtp_server.split(':')[1]))
        self.server.ehlo()
        self.server.starttls()
        # self.server = smtplib.SMTP_SSL(smtp_server, timeout=10)
        # self.server.ehlo()
        self.server.login(user, password)

        self.msg = MIMEMultipart()
        self.msg['From'] = fromaddr
        self.msg['To'] = COMMASPACE.join(toaddr)
        self.msg['Date'] = formatdate(localtime=True)

    def set_subject(self, subject):
        """
        set the subject of a email
        :param subject:
        :return:
        """
        self.msg['Subject'] = subject

    def set_bodytext(self, bodytext):
        """
        set the body text of a email
        :param bodytext: the text of email body
        :return:
        """
        self.msg.attach(MIMEText(bodytext, 'plain'))

    def add_attachment(self, file_list):
        """
        add attachments
        :param file_list: list of the paths of files with its extension
        :return:
        """
        for file_path in file_list:
            attachment = open(file_path, "rb").read()
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', os.path.basename(file_path)))
            self.msg.attach(part)

    def send(self):
        """
        send a email
        :return:
        """
        text = self.msg.as_string()
        self.server.sendmail(self.fromaddr, self.toaddr, text)


def remove_duplicate_data(x):
    """
    除去每个店铺的每个商品同一天的重复数据（只选取当日最新的数据，爬虫可能在当日内对一个商品重复爬取）
    :param x:
    :return:
    """
    result = []
    latest_date = ''
    for data in x:
        pid = data[0]
        info = data[1]
        shopId = None
        shopName = None
        data_set = dict()
        earliest_date = ''
        for i in info:
            status = i['status']
            if 'shopId' in i and i['shopId'] is not None:
                shopId = i['shopId']
            if 'shopName' in i and i['shopName'] is not None:
                shopName = i['shopName']
            price = i['price'] if 'price' in i else None  # 商名标准价格
            priceSales = i['priceSales'] if 'priceSales' in i else None  # 商名其他价格（促销，多个不同配置规格的商品）
            soldQuantity = i['soldQuantity'] if 'soldQuantity' in i else None
            fetchedAt = i['fetchedAt']
            date = fetchedAt.split(' ')[0]

            earliest_date = date if date < earliest_date or earliest_date == '' else earliest_date
            latest_date = date if date > latest_date or latest_date == '' else latest_date
            if status == '0':
                data_set[date] = {'status': '0', 'fetchedAt': fetchedAt, 'price': None, 'soldQuantity': None}
            else:
                if priceSales is None and price is None:
                    normed_price = None
                elif priceSales is not None and price is None:
                    normed_price = priceSales
                elif priceSales is None and price is not None:
                    normed_price = price
                else:
                    normed_price = price if priceSales == 0.0 else priceSales

                if shopId is None or shopName is None or normed_price is None or soldQuantity is None:
                    continue

                if date not in data_set:
                    data_set[date] = {'status': '1', 'price': normed_price, 'fetchedAt': fetchedAt, 'soldQuantity': soldQuantity}
                elif date in data_set and data_set[date]['fetchedAt'] < fetchedAt:
                    data_set[date]['status'] = '1'
                    data_set[date]['fetchedAt'] = fetchedAt
                    data_set[date]['price'] = normed_price
                    data_set[date]['soldQuantity'] = soldQuantity

        # 修补缺失的数据
        date_range = (datetime.datetime.strptime(latest_date, '%Y-%m-%d').date() -
                      datetime.datetime.strptime(earliest_date, '%Y-%m-%d').date()).days
        amend_step = 0  # 单个商品连续多天补齐数据的最大次数，不能超过5次
        for i in range(date_range):
            date1 = str(datetime.datetime.strptime(earliest_date, '%Y-%m-%d').date() + datetime.timedelta(days=i + 1))
            date2 = str(datetime.datetime.strptime(earliest_date, '%Y-%m-%d').date() + datetime.timedelta(days=i))
            if date1 in data_set:
                amend_step = 0
            else:
                if date2 in data_set and amend_step <= 5 and data_set[date2]['status'] == '1':
                    data_set[date1] = data_set[date2]
                    amend_step = amend_step + 1
                else:
                    break

        for i in data_set:
            if data_set[i]['status'] == '1':
                result.append(
                    {'pid': pid,
                     'shopId': shopId,
                     'shopName': shopName,
                     'price': data_set[i]['price'],
                     'soldQuantity': data_set[i]['soldQuantity'],
                     'date': i
                     })
    return result


def cacul_brand_sales_volume(x):
    """
    计算每个店铺当日的销售额
    :param x:
    :return:
    """
    result = []
    for data in x:
        shopId = data[0]
        info = data[1]
        shopName = ''
        data_set = dict()
        for i in info:
            shopName = i['shopName']
            date = i['date']
            price = i['price']
            soldQuantity = i['soldQuantity']

            if date not in data_set:
                data_set[date] = price * soldQuantity
            else:
                data_set[date] += price * soldQuantity
        for i in data_set:
            result.append({'shopId': shopId, 'shopName': shopName, 'date': i, 'sales_volume': data_set[i]})
    return result


def get_notify_shops(x):
    """
    计算出店铺的昨天和前天的销售量的浮动的比值，如果该比值大于设置的报告阈值，则将该店铺标记为通知状态。如果该店铺的昨天和前天的数据缺失，则也
    将该店铺标记为通知状态。
    :param x:
    :return:
    """

    notify_ratio = 0.4  # 当某个店铺的销售额变动大于 30% 时（一般情况是由于爬虫丢失数据），标记为通知状态

    result = []
    for data in x:
        shopId = data[0]
        info = data[1]
        shopName = ''
        data_set = dict()
        for i in info:
            shopName = i['shopName']
            date = i['date']
            sales_volume = i['sales_volume']
            data_set[date] = sales_volume

        yesterday = str(datetime.datetime.now().date() - datetime.timedelta(days=1))
        two_days_ago = str(datetime.datetime.now().date() - datetime.timedelta(days=2))
        if yesterday not in data_set:
            result.append({
                "shopId": shopId,
                "shopName": shopName,
                "date": yesterday,
                "sales_change_ratio": 0,
                "lost_data": True,
            })
        elif two_days_ago not in data_set:
            float_ratio = abs(data_set[yesterday] - data_set[two_days_ago])/(data_set[two_days_ago]+1)
            if float_ratio > notify_ratio:
                result.append({
                    "shopId": shopId,
                    "shopName": shopName,
                    "date": yesterday,
                    "sales_change_ratio": float_ratio,
                    "lost_data": False,
                })
    return result


def write_to_mongo(x):
    for data in x:
        client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
        db = client['cr_data']
        db.authenticate(USER, PASSWORD)
        collection = db['tmall_sales_volume']
        _id = data['date'] + '_' + data['stock_code']
        op = {
            'date': data['date'],
            'sales_volume': data['sales_volume'],
            'shopId': data['shopId'],
            'shopName': data['shopName'],
            'brand': data['brand'],
            'stock_code': data['stock_code'],
            'last_updated': datetime.datetime.now()
        }
        collection.update_one({'_id': _id}, {'$set': op}, upsert=True)


if __name__ == '__main__':
    conf = SparkConf().setAppName("Tmall_Brand_Sales_Volume")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')

    # 计算出每日店铺销售额
    df = sparkSession.sql("SELECT pid, shopId, shopName, price, priceSales, soldQuantity, fetchedAt, status "
                          "FROM spider_data.tmall_product_v2 "
                          "WHERE pid is not null AND fetchedAt is not null AND status is not null")

    rdd1 = df.rdd.groupBy(lambda x: x['pid']).mapPartitions(lambda x: remove_duplicate_data(x))
    rdd2 = rdd1.groupBy(lambda x: x['shopId']).mapPartitions(lambda x: cacul_brand_sales_volume(x))

    shop_sales_volume_df = rdd2.toDF()
    shop_sales_volume_df.registerTempTable('table1')
    shop_mappings_df = sparkSession.sql("SELECT * FROM abc.shop_mappings")
    shop_mappings_df.registerTempTable('table2')

    result_df = sparkSession.sql("SELECT table1.date, table1.sales_volume, table1.shopId, table1.shopName, "
                                 "table2.brand, table2.stock_code "
                                 "FROM table1 JOIN table2 ON table1.shopId = table2.shopId")

    result_df.show(100)

    # 将每日店铺销售额写入 MongoDB
    result_df.rdd.foreachPartition(lambda x: write_to_mongo(x))

    # 将每日需要报告的信息通过邮件发送
    rdd3 = result_df.rdd.groupBy(lambda x: x['shopId']).mapPartitions(lambda x: get_notify_shops(x))
    notify_shops = rdd3.collect()

    if len(notify_shops) > 0:
        table_header = ['date', 'shopId', 'ratio', 'lost', '']
        row_format = "{:>10}{:>10}{:>10}{:>10}{:>30}"
        message = row_format.format(*table_header)
        notify_data = []
        for i in notify_shops:
            notify_data.append([i['date'], i['shopId'], format(i['sales_change_ratio'], '.4f'),
                                str(i['lost_data']), i['shopName']])

        for i in notify_data:
            message += '\n' + row_format.format(*i)

        yesterday_date = str(datetime.datetime.now().date() - datetime.timedelta(days=1))
        message = yesterday_date + ' 所有需要注意的店铺状态：\n\n' + message
        print(message)
        email = MyEmail('mail.niub.la:465', 'chyan@abcft.com', ['chyan@abcft.com', 'jili@abcft.com'],
                        'chyan.abcft@niub', 'Ytn3Lvc4')
        email.set_subject('Tmall 爬虫爬取店铺情况')
        email.set_bodytext(message)
        email.send()

