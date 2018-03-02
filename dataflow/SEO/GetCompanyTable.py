from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as sqlf
from pyspark.sql.types import *
import hashlib
import pshc


if __name__ == '__main__':

    conf = SparkConf().setAppName("GetCompanyTable")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder\
        .enableHiveSupport() \
        .config(conf=conf)\
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')

    connector = pshc.PSHC(sc, sqlContext)

    info_catelog = {
        "table": {"namespace": "default", "name": "SEO_info"},
        "rowkey": "id",
        "columns": {
            "id": {"cf": "data", "col": "id", "type": "string"},  # 图片 id
            "stockcode": {"cf": "data", "col": "industry_id", "type": "string"},
            "create_time": {"cf": "data", "col": "create_time", "type": "string"},
        }
    }

    company_table_df = connector.get_df_from_hbase(info_catelog).persist(storageLevel=StorageLevel.DISK_ONLY)

    # 除去industry_id为空的row，加上index列
    company_table_rdd = company_table_df.filter('stockcode != ""')\
        .orderBy(["stockcode", "create_time"], ascending=[1, 0])\
        .rdd.zipWithIndex().map(lambda x: (x[0]['id'], x[0]['stockcode'], x[0]['create_time'], x[1]))

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("stockcode", StringType(), True),
        StructField("create_time", StringType(), True),
        StructField("index", IntegerType(), True)
    ])
    company_table_df = sqlContext.createDataFrame(company_table_rdd, schema=schema)

    print('----company_table_df COUNT:---\n', company_table_df.count())
    company_table_df.show(20, False)

    # 计算出每个行业的index起始，结束和数量
    company_meta_df = company_table_df.groupBy('stockcode')\
                                      .agg(sqlf.min('index'), sqlf.max('index'), sqlf.count('index'))\
                                      .toDF('stockcode', 'min', 'max', 'count')

    print('----company_meta_df COUNT:---\n', company_meta_df.count())
    company_meta_df.show(20, False)

    # 计算出 industry_imgs_df
    company_table_df.registerTempTable('company_table_df')
    company_meta_df.registerTempTable('company_meta_df')
    page_num = 12

    def hash_id(id):
        return hashlib.md5(bytes(id, encoding="utf-8")).hexdigest()[0:10] + ':' + id

    company_imgs_df = sparkSession.sql(
        "select company_table_df.id, company_table_df.industry_id, company_table_df.create_time, "
        "company_table_df.index, company_meta_df.min, company_meta_df.max, company_meta_df.count "
        "from company_table_df join company_meta_df on company_table_df.stockcode="
        "company_meta_df.stockcode order by stockcode, create_time DESC")\
        .rdd.map(lambda x: (x['stockcode'] + '_' + str((x['index'] - x['min'] + 1) // page_num), x['id']))\
        .reduceByKey(lambda x, y:  hash_id(str(x)) + ',' + hash_id(str(y))).toDF(['company_paging', 'img_ids'])

    print('----company_imgs_df COUNT:---\n', company_imgs_df.count())
    company_imgs_df.show(20, False)

    # 将 result_df 保存至 Hbase
    company_imgs_catelog = {
        "table": {"namespace": "default", "name": "SEO_company"},
        "rowkey": "company_paging",
        "columns": {
            "company_paging": {"cf": "rowkey", "col": "key", "type": "string"},
            "img_ids": {"cf": "data", "col": "img_ids", "type": "string"},
        }
    }

    connector.save_df_to_hbase(company_imgs_df, company_imgs_catelog)






