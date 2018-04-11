import datetime
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession
import datetime
import pymongo

OFF_LINE_MONGO = '10.12.0.30'
OFF_LINE_MONGO_PORT = 27017


def remove_duplicate_data(data):
    """
    删除同一天里 pid 重复的数据，调整 price
    :param data:
    :return:
    """
    product_time_price = dict()
    for x in data:
        pid = x['pid']
        time = x['fetchedAt']
        price = x['price']
        priceSales = x['priceSales']
        if pid not in product_time_price:
            product_time_price[pid] = (time, price) if priceSales == 0.0 else (time, priceSales)
        else:
            if product_time_price[pid][0] < time:
                product_time_price[pid] = (time, price) if priceSales == 0.0 else (time, priceSales)
    result = []
    for y in product_time_price:
        item = {
            'pid': y,
            'date': product_time_price[y][0].split(' ')[0],
            'price': product_time_price[y][1]
        }
        result.append(item)
    return result


def fun1(x):
    date1 = x['date']
    products1 = x['products']['data']
    date2 = x['date2']
    products2 = x['products2']['data']
    dataset = dict()
    for i in products1:
        if i['pid'] not in dataset:
            dataset[i['pid']] = {}
        dataset[i['pid']][i['date']] = i['price']
    for i in products2:
        if i['pid'] not in dataset:
            dataset[i['pid']] = {}
        dataset[i['pid']][i['date']] = i['price']
    sum = 0
    num = 0
    print(dataset)
    for i in dataset:
        if len(dataset[i]) == 2:
            sum += dataset[i][date2]/dataset[i][date1] if dataset[i][date1] != 0.0 else 1
            num += 1
    ratio = sum/num
    return {'date': date2, 'ratio': ratio}


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
                          "WHERE pid is not null AND shopId is not null AND price is not null "
                          "AND priceSales is not null AND fetchedAt is not null ")

    df.registerTempTable('table1')
    shopId_df = sparkSession.sql("SELECT DISTINCT(shopId), shopName FROM table1")
    shopId_list = shopId_df.rdd.map(lambda x: (x['shopId'], x['shopName'])).collect()

    for shopId, shopName in shopId_list:
        shop_df_raw = sparkSession.sql("SELECT pid, price, priceSales, fetchedAt FROM table1 WHERE shopId = " + shopId)
        shop_df = shop_df_raw.rdd.mapPartitions(lambda x: remove_duplicate_data(x)).toDF()
        shop_df.registerTempTable('shop_table')
        date_list = sparkSession.sql("SELECT DISTINCT(date) FROM shop_table ORDER BY date").rdd.map(lambda x: x[0]).collect()

        shop_daily_df = shop_df.rdd.groupBy(lambda x: x['date']).toDF(['date', 'products'])
        shop_daily_df.registerTempTable('shop_daily1')
        shop_daily_df2 = sparkSession.sql('SELECT date as date2, products as products2 FROM shop_daily1')
        shop_daily_df2.registerTempTable('shop_daily2')

        shop_day_pair_df = sparkSession.sql("SELECT * FROM shop_daily1 JOIN shop_daily2 ON DATE_ADD(shop_daily1.date,1)= shop_daily2.date2")
        shop_daily_ratio = shop_day_pair_df.rdd.map(lambda x: fun1(x)).collect()

        ratio_dict = dict()
        for i in shop_daily_ratio:
            ratio_dict[i['date']] = i['ratio']

        earlist_day = datetime.datetime.strptime(date_list[0], '%Y-%m-%d').date()
        latest_day = datetime.datetime.strptime(date_list[-1], '%Y-%m-%d').date()
        cusor_date = earlist_day
        result = {}
        while cusor_date <= latest_day:
            if cusor_date == earlist_day:
                result[str(cusor_date)] = 1.0
            elif str(cusor_date) in ratio_dict and str(cusor_date - datetime.timedelta(days=1)) in ratio_dict:
                result[str(cusor_date)] = result[str(cusor_date - datetime.timedelta(days=1))] * ratio_dict[str(cusor_date)]
            else:
                result[str(cusor_date)] = result[str(cusor_date - datetime.timedelta(days=1))] * 1.0
            cusor_date += datetime.timedelta(days=1)

        client = pymongo.MongoClient(OFF_LINE_MONGO, OFF_LINE_MONGO_PORT)
        collection = client['research']['tmall_brand_index']

        for i in result:
            _id = shopId
            put_data = {'shopName': shopName, 'date': i, 'index': result[i]}
            collection.update_one({'_id':_id}, {'$set': put_data}, upsert=True)






























