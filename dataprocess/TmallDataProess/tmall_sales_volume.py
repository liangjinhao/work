import datetime
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
            soldQuantity = i['soldQuantity']
            date = fetchedAt.split(' ')[0]
            if date not in data_set:
                normed_price = price if priceSales == 0.0 else priceSales
                data_set[date] = {'price': normed_price, 'fetchedAt': fetchedAt, 'soldQuantity': soldQuantity}
            elif date in data_set and data_set[date]['fetchedAt'] < fetchedAt:
                data_set[date]['fetchedAt'] = fetchedAt
                normed_price = price if priceSales == 0.0 else priceSales
                data_set[date]['price'] = normed_price
                data_set[date]['soldQuantity'] = soldQuantity
        for i in data_set:
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
    conf = SparkConf().setAppName("TmallBrandSalesVolume")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')

    df = sparkSession.sql("SELECT pid, shopId, shopName, price, priceSales, soldQuantity, fetchedAt "
                          "FROM spider_data.tmall_product "
                          "WHERE pid is not null AND shopId is not null AND shopName is not null "
                          "AND price is not null AND priceSales is not null AND soldQuantity is not null "
                          "AND fetchedAt is not null ")

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

    result_df.rdd.foreachPartition(lambda x: write_to_mongo(x))

