
# 单结点提交
nohup /usr/local/spark2/bin/spark-submit --master local --conf spark.pyspark.virtualenv.enabled=true --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.bin.path=/usr/bin/virtualenv --conf spark.pyspark.python=/home/chyan/virtualenv/default/bin/python --py-files /home/chyan/work/time_norm/HBase_Tool.py,/home/chyan/work/time_norm/Utility.py  --files /home/chyan/work/time_norm/abst_title.model,/home/chyan/work/time_norm/abst_title.model.wv.syn0.npy,/home/chyan/work/time_norm/abst_title.model.syn1neg.npy,/home/chyan/work/time_norm/time.model /home/chyan/work/time_norm/TimeNorm.py --executor-memory 2G --total-executor-cores 2 >> /home/chyan/work/time_norm/time.log


# 集群提交

nohup /usr/local/spark2/bin/spark-submit spark://abc-cloudera004:7077 --conf spark.pyspark.virtualenv.enabled=true --conf spark.pyspark.virtualenv.type=native --conf spark.pyspark.virtualenv.bin.path=/usr/bin/virtualenv --conf spark.pyspark.python=/home/chyan/virtualenv/default/bin/python --py-files /home/chyan/work/time_norm/HBase_Tool.py,/home/chyan/work/time_norm/Utility.py  --files /home/chyan/work/time_norm/abst_title.model,/home/chyan/work/time_norm/abst_title.model.wv.syn0.npy,/home/chyan/work/time_norm/abst_title.model.syn1neg.npy,/home/chyan/work/time_norm/time.model /home/chyan/work/time_norm/TimeNorm.py --executor-memory 2G --total-executor-cores 2 >> /home/chyan/work/time_norm/time.log 2>&1 &

spark-submit --master yarn --conf spark.pyspark.python=/mnt/disk1/data/chyan/virtualenv/bin/python --py-files /mnt/disk1/data/chyan/work/time_norm/abc_time.py,/mnt/disk1/data/chyan/work/time_norm/HBaseTool.py,/mnt/disk1/data/chyan/work/time_norm/Utility.py /mnt/disk1/data/chyan/work/time_norm/TimeNorm.py --executor-memory 2G --num-executors 4 --executor-cores 2