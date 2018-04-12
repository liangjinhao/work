from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import datetime
import pymongo

# 国内 MongoDB 连接信息
MONGODB_HOST = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
MONGODB_PORT = 3717
USER = 'search'
PASSWORD = 'ba3Re3ame+Wa'


def remove_duplicate_data(x):
    result = []
    for data in x:
        pid = data[0]
        info = data[1]
        shopId = ''
        shopName = ''
        data_set = dict()
        for i in info:
            shopId = i['shopId']
            shopName = i['shopName']
            price = i['price']
            priceSales = i['priceSales']
            fetchedAt = i['fetchedAt']
            date = fetchedAt.split(' ')[0]
            if date not in data_set:
                normed_price = price if priceSales == 0.0 else priceSales
                data_set[date] = {'price': normed_price, 'fetchedAt': fetchedAt}
            elif date in data_set and data_set[date]['fetchedAt'] < fetchedAt:
                data_set[date]['fetchedAt'] = fetchedAt
                normed_price = price if priceSales == 0.0 else priceSales
                data_set[date]['price'] = normed_price
        for i in data_set:
            result.append(
                {'pid': pid, 'shopId': shopId, 'shopName': shopName, 'price': data_set[i]['price'], 'date': i})
    return result


def cacul_brand_index(x):
    result = []
    for data in x:
        shopId = data[0]
        info = data[1]
        shopName = ''
        data_set = dict()
        earliest_day = ''
        lastest_day = ''
        for i in info:
            shopName = i['shopName']
            date = i['date']
            pid = i['pid']
            price = i['price']
            if earliest_day == '' or earliest_day > date:
                earliest_day = date
            if lastest_day == '' or lastest_day < date:
                lastest_day = date
            if date not in data_set:
                data_set[date] = {pid: price}
            else:
                data_set[date][pid] = price
        result.append({'shopId': shopId, 'shopName': shopName, 'date': earliest_day, 'ratio': 1.0})
        cursor_day = datetime.datetime.strptime(earliest_day, '%Y-%m-%d').date() + datetime.timedelta(days=1)
        while str(cursor_day) <= lastest_day:
            if str(cursor_day) in data_set and str(cursor_day - datetime.timedelta(days=1)) in data_set:
                last_day_data = data_set[str(cursor_day - datetime.timedelta(days=1))]
                today_data = data_set[str(cursor_day)]
                sum = 0
                num = 0
                for i in today_data:
                    if i in last_day_data:
                        sum += today_data[i] / last_day_data[i] if last_day_data[i] != 0 else 1
                        num += 1
                ratio = sum / num if num != 0 else 1.0
                result.append({'shopId': shopId, 'shopName': shopName, 'date': str(cursor_day), 'ratio': ratio})
            else:
                result.append({'shopId': shopId, 'shopName': shopName, 'date': str(cursor_day), 'ratio': 1.0})
            cursor_day += datetime.timedelta(days=1)
    return result


def write_to_mongo(x):
    for data in x:
        client = pymongo.MongoClient(MONGODB_HOST, MONGODB_PORT)
        db = client['cr_data']
        db.authenticate(USER, PASSWORD)
        collection = db['tmall_brand_index']
        _id = data['stock_code']
        op = {
            'date': data['date'],
            'ratio': data['ratio'],
            'shopId': data['shopId'],
            'shopName': data['shopName'],
            'brand': data['brand']
        }
        collection.update_one({'_id': _id}, {'$set': op}, upsert=True)


if __name__ == '__main__':
    conf = SparkConf().setAppName("TmallDataProcess")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')

    df = sparkSession.sql("SELECT pid, shopId, shopName, price, priceSales, fetchedAt "
                          "FROM spider_data.tmall_product "
                          "WHERE pid is not null AND shopId is not null AND shopName is not null "
                          "AND price is not null AND priceSales is not null AND fetchedAt is not null ")

    rdd1 = df.rdd.groupBy(lambda x: x['pid']).mapPartitions(lambda x: remove_duplicate_data(x))
    rdd2 = rdd1.groupBy(lambda x: x['shopId']).mapPartitions(lambda x: cacul_brand_index(x))

    shop_index_df = rdd2.toDF()
    shop_index_df.registerTempTable('table1')
    shop_mappings_df = sparkSession.sql("SELECT * FROM abc.shop_mappings")
    shop_mappings_df.registerTempTable('table2')

    result_df = sparkSession.sql("SELECT table1.date, table1.ratio, table1.shopId, table1.shopName, "
                                 "table2.brand table2.stock_code"
                                 "FROM table1 JOIN table2 WHERE table1.shopId = table2.shopId")

    result_df.show()

    result_df.rdd.foreachPartition(lambda x: write_to_mongo(x))
