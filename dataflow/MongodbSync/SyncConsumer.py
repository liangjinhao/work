import logging
import MongoDBPusher
import OSSPusher

if __name__ == '__main__':

    for i in range(40):
        MongoDBPusher.MongoDBPusher().start()
        print('MongoDB 推送线程 %s 开启', i)

    for i in range(20):
        OSSPusher.OSSPusher().start()
        print('OSS 推送线程 %s 开启', i)
