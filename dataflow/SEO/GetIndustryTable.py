from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext, SparkSession
import pyspark.sql.functions as sqlf
from pyspark.sql.types import *
import hashlib
import pshc


if __name__ == '__main__':

    conf = SparkConf().setAppName("GetIndustryTable")
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
            "industry_id": {"cf": "data", "col": "industry_id", "type": "string"},
            "create_time": {"cf": "data", "col": "create_time", "type": "string"},
        }
    }

    industry_table_df = connector.get_df_from_hbase(info_catelog).persist(storageLevel=StorageLevel.DISK_ONLY)

    # 除去industry_id为空的row，加上index列
    industry_table_rdd = industry_table_df.filter('industry_id != ""')\
        .orderBy(["industry_id", "create_time"], ascending=[1, 0])\
        .rdd.zipWithIndex()\
        .map(lambda x: (x[0]['id'], x[0]['industry_id'], x[0]['create_time'], x[1]))\
        .toDF(['id', 'industry_id', 'create_time', 'index'])

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("industry_id", StringType(), True),
        StructField("create_time", StringType(), True),
        StructField("index", IntegerType(), True)
    ])
    industry_table_df = sqlContext.createDataFrame(industry_table_rdd, schema=schema)

    print('----industry_table_df COUNT:---\n', industry_table_df.count())
    industry_table_df.show(20, False)

    # 计算出每个行业的index起始，结束和数量
    industry_meta_df = industry_table_df.groupBy('industry_id')\
                                        .agg(sqlf.min('index'), sqlf.max('index'), sqlf.count('index'))\
                                        .toDF('industry_id', 'min', 'max', 'count')

    print('----industry_meta_df COUNT:---\n', industry_meta_df.count())
    industry_meta_df.show(20, False)

    # 计算出 industry_imgs_df
    industry_table_df.registerTempTable('industry_table_df')
    industry_meta_df.registerTempTable('indus_meta_df')
    page_num = 12

    def hash_id(id):
        return hashlib.md5(bytes(id, encoding="utf-8")).hexdigest()[0:10] + ':' + id

    industry_imgs_df = sparkSession.sql(
        "select industry_table_df.id, industry_table_df.industry_id, industry_table_df.create_time, "
        "industry_table_df.index, industry_meta_df.min, industry_meta_df.max, industry_meta_df.count "
        "from industry_table_df join industry_meta_df on industry_table_df.industry_id="
        "industry_meta_df.industry_id order by industry_id, create_time DESC")\
        .rdd.map(lambda x: (x['industry_id'] + '_' + str((x['index'] - x['min'] + 1) // page_num), x['id']))\
        .reduceByKey(lambda x, y: hash_id(str(x)) + ',' + hash_id(str(y))).toDF(['industry_paging', 'img_ids'])

    print('----industry_imgs_df COUNT:---\n', industry_imgs_df.count())
    industry_imgs_df.show(20, False)

    # 将 result_df 保存至 Hbase
    industry_imgs_catelog = {
        "table": {"namespace": "default", "name": "SEO_industry"},
        "rowkey": "industry_paging",
        "columns": {
            "industry_paging": {"cf": "rowkey", "col": "key", "type": "string"},
            "img_ids": {"cf": "data", "col": "img_ids", "type": "string"},
        }
    }

    connector.save_df_to_hbase(industry_imgs_df, industry_imgs_catelog)
