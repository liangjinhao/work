import MongoDBListener
import logging


"""
国内 Mongo 集群和国外 Mongo 集群同步中的生产端(运行在国内的 bj-mogo-sync002 上)：

1. 负责监控国内 Mongo 集群的更新日志 oplog，将所有的 MongoDB 更新操作写入 MongoDB 消息队列
2. 同时，如果 Mongo 更新操作的表的字段里有 OSS 链接，将这些 OSS 链接也写入 OSS 的消息队列

"""


if __name__ == '__main__':

    listener = MongoDBListener.MongoDBListener()
    listener.start()
    print('MongoDB 监听线程开启')
