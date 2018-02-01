from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
import logging
from logging.handlers import RotatingFileHandler
import pika


handle = RotatingFileHandler('./img_statisc.log', maxBytes=5*1024*1024, backupCount=1)
handle.setLevel(logging.WARNING)
log_formater = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
handle.setFormatter(log_formater)

logger = logging.getLogger('Rotating log')
logger.addHandler(handle)
logger.setLevel(logging.WARNING)

# thrift
host = '10.27.71.108'
port = 9099
transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
transport.open()

# Rabbitmq
vhost = 'search'  # RabbitMQ 虚拟主机
username = 'search'  # RabbitMQ 用户名
password = '0dx8iYF3rII91YRe'  # RabbitMQ 密码
RabbitMQ_ip = '47.98.34.75'  # RabbitMQ IP地址
RabbitMQ_port = 5672  # RabbitMQ 端口
queue_name = 'scrawl_images'  # RabbitMQ 送入的队列名字
credentials = pika.PlainCredentials(username, password)
connection = pika.BlockingConnection(
    pika.ConnectionParameters(host=RabbitMQ_ip, port=RabbitMQ_port,
                              virtual_host=vhost, credentials=credentials))
channel = connection.channel()
channel.queue_declare(
    queue=queue_name,
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": queue_name,
    }
)

# scan = TScan(startRow=b'3bd22fc949:1989906_8_4')
scan = TScan()
scanner = client.scannerOpenWithScan(b'img_data', scan, None)
result = client.scannerGet(scanner)

all_count = 0
type_count = {}
not_processed = 0
deleted = 0

f = open('img_url.txt', 'w')

while result:
    all_count += 1

    rowkey = str(result[0].row, encoding='utf-8').split(':')[-1]
    if b'info:img_url' in result[0].columns:
        img_url = str(result[0].columns[b'info:img_url'].value, encoding='utf-8')
    else:
        client.deleteAllRow(b'img_data', result[0].row, attributes=None)
        deleted += 1
        result = client.scannerGet(scanner)
        continue

    if b'info:img_oss' in result[0].columns:
        img_oss = str(result[0].columns[b'info:img_oss'].value, encoding='utf-8')
    else:
        print(rowkey, 'has no img_oss,    ', result[0])
        result = client.scannerGet(scanner)
        continue

    if b'info:img_type' in result[0].columns:
        img_type = str(result[0].columns[b'info:img_type'].value, encoding='utf-8')

        if img_type == "[]":
            type_count['wrong'] = type_count['wrong'] + 1 if 'wrong' in type_count else 1
            message = str(result[0].columns[b'info:img_oss'].value, encoding='utf-8')
            channel.basic_publish(exchange='',
                                  routing_key=queue_name,
                                  body=message,
                                  properties=pika.BasicProperties(
                                      delivery_mode=2,  # make message persistent
                                  ))
        else:
            img_type = eval(img_type)

            other_flag = False
            for i in img_type:
                if i['type'] == 'OTHER':
                    other_flag = True
            if other_flag:
                type_count['other'] = type_count['other'] + 1 if 'other' in type_count else 1
                message = str(result[0].columns[b'info:img_oss'].value, encoding='utf-8')
                channel.basic_publish(exchange='',
                                      routing_key=queue_name,
                                      body=message,
                                      properties=pika.BasicProperties(
                                          delivery_mode=2,  # make message persistent
                                      ))
            else:
                type_count['determined'] = type_count['determined'] + 1 if 'determined' in type_count else 1
                message = {'img_url': img_url, 'img_oss': img_oss, 'img_type': img_type}
                f.write(str(message) + '\n')
    else:
        message = str(result[0].columns[b'info:img_oss'].value, encoding='utf-8')
        channel.basic_publish(exchange='',
                              routing_key=queue_name,
                              body=message,
                              properties=pika.BasicProperties(
                                  delivery_mode=2,  # make message persistent
                              ))
        not_processed += 1

    result = client.scannerGet(scanner)


logging.warning('删除数据量： ' + str(deleted) + '    ' + '\n' +
                '所有数据量： ' + str(all_count) + '    ' + '\n' +
                '未被处理的数据量： ' + str(not_processed) + '\n' +
                '处理出错的所有数据量： ' + str(type_count['wrong']) + '    ' + str(type_count['wrong']/(all_count-not_processed)) + '\n' +
                '判别成功的数据量： ' + str(type_count['determined']) + '    ' + str(type_count['determined']/(all_count-not_processed)) + '\n' +
                '判别Other的数据量： ' + str(type_count['other']) + '    ' + str(type_count['other']/(all_count-not_processed)) + '\n')

f.close()
transport.close()
connection.close()
