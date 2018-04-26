from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession


if __name__ == '__main__':
    conf = SparkConf().setAppName("WriteSiteRankToHive")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    sparkSession = SparkSession.builder \
        .enableHiveSupport() \
        .config(conf=conf) \
        .getOrCreate()
    sparkSession.sparkContext.setLogLevel('WARN')

    site_rank = dict()

    with open('site_rank.txt', 'r') as f:
        for line in f:
            site_to_rank = line.strip().split('\t')
            if len(site_to_rank) == 2:
                site_rank[site_to_rank[0]] = float(site_to_rank[1])
    dataset = []
    for i in site_rank:
        dataset.append({'site': i, 'rank': site_rank[i]})
    df = sc.parallelize(dataset).toDF()
    df.write.saveAsTable('abc.site_rank')
