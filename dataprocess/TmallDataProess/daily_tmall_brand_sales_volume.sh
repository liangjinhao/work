#!/bin/bash

cd /mnt/disk1/data/chyan/work/dataprocess/TmallDataProess/
source /etc/profile
nohup spark-submit --master yarn --executor-memory 4G --executor-cores 2 --num-executors 5 --packages org.apache.hbase:hbase:1.1.12,org.apache.hbase:hbase-server:1.1.12,org.apache.hbase:hbase-client:1.1.12,org.apache.hbase:hbase-common:1.1.12 --jars /mnt/disk1/data/chyan/work/dataflow/spark-examples_2.10-1.6.4-SNAPSHOT.jar --conf spark.pyspark.python=/mnt/disk1/data/chyan/virtualenv/bin/python --conf spark.yarn.executor.memoryOverhead=1024m --conf spark.executorEnv.PYTHONHASHSEED=321 --conf spark.sql.catalogImplementation=hive /mnt/disk1/data/chyan/work/dataprocess/TmallDataProess/tmall_brand_sales_volume.py &