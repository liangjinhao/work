from pyspark import SparkConf, SparkContext, StorageLevel
from pyspark.sql import SQLContext, SparkSession
import pshc


def expand(row):
    new_row = list()
    new_row.append(row[0])
    for sub_row in row[1]:
        tmp = list()
        for i in sub_row:
            if i != '':
                tmp.append(i)
        new_row.append(tmp)
    return tuple(new_row)


if __name__ == '__main__':

    conf = SparkConf().setAppName("GetCompanyMetaTable")
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
            "id": {"cf": "rowkey", "col": "key", "type": "string"},
            "stockcode": {"cf": "data", "col": "stockcode", "type": "string"},
            "stockname": {"cf": "data", "col": "stockname", "type": "string"},
            "industry_id": {"cf": "data", "col": "industry_id", "type": "string"},
            "industry": {"cf": "data", "col": "industry", "type": "string"},
            "publish": {"cf": "data", "col": "publish", "type": "string"},
        }
    }

    company_table_df = connector.get_df_from_hbase(info_catelog).persist(storageLevel=StorageLevel.DISK_ONLY)

    print('----company_table_df COUNT:---\n', company_table_df.count())
    company_table_df.show(20, False)

    company_meta_table = company_table_df.select('stockcode, stockname, industry_id, industry, publish')\
        .filter('stockcode != "" and industry_id != ""')\
        .rdd \
        .map(lambda x: (x[0], x[1], x[2] + '_' + x[3], x[4])) \
        .map(lambda x: (x[0], ([x[1]], [x[2]], [x[3]]))) \
        .reduceByKey(lambda x, y: (list(set(x[0] + y[0])), list(set(x[1] + y[1])), list(set(x[2] + y[2])))) \
        .map(expand) \
        .map(lambda x: (x[0], x[1][0], x[2], x[3])) \
        .toDF(['stockcode', 'stockname', 'industries', 'publishers'])

    print('----company_meta_table COUNT:---\n', company_meta_table.count())
    company_meta_table.show(20, False)

    # 将 industry_meta_df 保存至 Hbase
    company_meta_catelog = {
        "table": {"namespace": "default", "name": "SEO_company_meta"},
        "rowkey": "company_id",
        "columns": {
            "company_id": {"cf": "rowkey", "col": "key", "type": "string"},
            "company": {"cf": "data", "col": "company", "type": "string"},
            "industries": {"cf": "data", "col": "industries", "type": "string"},
            "publishers": {"cf": "data", "col": "publishers", "type": "string"},
        }
    }

    connector.save_df_to_hbase(company_meta_table, company_meta_catelog)