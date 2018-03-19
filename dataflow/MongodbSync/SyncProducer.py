import MongoDBListener
import logging

if __name__ == '__main__':

    listener = MongoDBListener.MongoDBListener()
    listener.start()
    print('MongoDB 监听线程开启')
