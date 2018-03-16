import logging
import MongoDBPusher
import OSSPusher

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s",
        datefmt="%m-%d %H:%M:%S"
    )

    for i in range(50):
        MongoDBPusher.MongoDBPusher().start()
        print('MongoDB 推送线程 %s 开启', i)

    for i in range(20):
        oss_pusher1 = OSSPusher.OSSPusher()
        oss_pusher1.start()
        print('OSS 推送线程 %s 开启', i)
