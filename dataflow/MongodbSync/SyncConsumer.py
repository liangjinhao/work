import logging
import MongoDBPusher
import OSSPusher


"""
国内 Mongo 集群和国外 Mongo 集群同步中的消费端(运行在香港的 bj-mogo-sync001 上)：

1. MongoDBPusher 线程从 Mongo 消息队列中取出国内 MongoDB 的历史操作并更新到国外 MongoDB，
2. OSSPusher 线程从 OSS 消息队列中取出国内 MongoDB 历史操作的表的字段里的 OSS 链接作并下载这些链接然后转存到国外 OSS服务中

注意，这里需要经过公网取 MongoDB 更新操作数据(每天基本在千万条以上)，又需要从国内 OSS 上下载文件到国外 OSS，所以所需网速带宽要大些，
否则会超时连接

"""


if __name__ == '__main__':

    for i in range(50):
        MongoDBPusher.MongoDBPusher().start()
        print('MongoDB 推送线程 %s 开启', i)

    for i in range(50):
        OSSPusher.OSSPusher().start()
        print('OSS 推送线程 %s 开启', i)
