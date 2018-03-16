from MongodbSync import MongoDBListener
import logging

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-12s %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s",
        datefmt="%m-%d %H:%M:%S"
    )

    listener = MongoDBListener.MongoDBListener()
    listener.start()
    print('MongoDB 监听线程开启')
