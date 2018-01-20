from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.ttypes import *
from pymongo import MongoClient
import logging
from logging.handlers import RotatingFileHandler


handle = RotatingFileHandler('./process_mongodb.log', maxBytes=5*1024*1024, backupCount=1)
handle.setLevel(logging.WARNING)
log_formater = logging.Formatter('%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
handle.setFormatter(log_formater)

logger = logging.getLogger('Rotating log')
logger.addHandler(handle)
logger.setLevel(logging.WARNING)

host = '10.27.71.108'
port = 9099
transport = TTransport.TBufferedTransport(TSocket.TSocket(host, port))
protocol = TBinaryProtocol.TBinaryProtocol(transport)
client = Hbase.Client(protocol)
transport.open()

user = 'search'
password = 'ba3Re3ame+Wa'
host = 'dds-bp1d09d4b278ceb41.mongodb.rds.aliyuncs.com'
port = 3717
db_name = 'cr_data'
table = 'hb_charts'

mongo_client = MongoClient(host, port)
db = mongo_client[db_name]
db.authenticate(user, password)
collection = db[table]

scan = TScan(startRow=b'3bd22fc949:1989906_8_4')

# scan.filterString = bytes("SingleColumnValueFilter('data', 'state', =, 'binary:3') OR "
#                           "SingleColumnValueFilter('data', 'state', =, 'binary:4') OR "
#                           "SingleColumnValueFilter('data', 'state', =, 'binary:5')", encoding='utf-8')
scanner = client.scannerOpenWithScan(b'hb_charts', scan, None)
result = client.scannerGet(scanner)

all_count = 0
delete_count = 0

while result:
    all_count += 1

    rowkey = str(result[0].row, encoding='utf-8').split(':')[-1]
    state = str(result[0].columns[b'data:state'].value, encoding='utf-8')

    count = collection.find({'_id': rowkey}).count()
    if count == 1 and state not in ['3', '4', '5']:
        # print(rowkey, 'in MongoDB and state is in use')
        pass
    else:
        # print(rowkey, result[0].row, 'not in MongoDB or state is not in use')
        client.deleteAllRow(b'hb_charts', result[0].row, attributes=None)
        delete_count += 1

    if all_count % 100000 == 0:
        logging.warning('已经扫描了' + str(all_count/10000) + '万条数据，删除了' + str(delete_count/10000) + '万条数据' +
                        '\nrowkey:' + str(result[0].row))

    result = client.scannerGet(scanner)

transport.close()
mongo_client.close()
