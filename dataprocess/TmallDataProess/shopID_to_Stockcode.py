from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession


if __name__ == '__main__':
    conf = SparkConf().setAppName("ShopIdMappings")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')
    shopId_to_Stockcode = []
    with open('/mnt/disk1/data/chyan/work/dataprocess/TmallDataProess/shop_mappings.txt') as f:
        for line in f:
            pairs = line.strip().split(' ')
            shopId = pairs[0]
            shopName = pairs[1]
            # 160 / 1100
            if len(pairs) == 4:
                stock_code = pairs[2].upper()
                brand = pairs[3]
                shopId_to_Stockcode.append(
                    {'shopId': shopId,
                     'shopName': shopName,
                     'stock_code': stock_code,
                     'brand': brand
                     })
    df = sc.parallelize(shopId_to_Stockcode).toDF()
    df.write.saveAsTable('abc.shop_mappings', mode='overwrite')

