import logging
import MongoDBPusher
import OSSPusher

if __name__ == '__main__':

    for i in range(100):
        MongoDBPusher.MongoDBPusher().start()
        print('MongoDB 推送线程 %s 开启', i)

    for i in range(50):
        oss_pusher1 = OSSPusher.OSSPusher()
        oss_pusher1.start()
        print('OSS 推送线程 %s 开启', i)
