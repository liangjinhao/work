import Producer
import Consumer

if __name__ == '__main__':

    p = Producer.ScrawlImagesProducer()
    p.start()
    print('生产线程开启')
    c = Consumer.ScrawlImagesConsumer()
    c.start()
    print('消费线程开启')

