from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.Hbase import *


class HBase_Self_tool:
    """
    用于在Hbase相关RDD中计算的函数，封装到一个类中方便管理和调用
    该类仅用于读取pyspark读取hbase时使用。
    以此类作为外部调用来避免出现不能序列化的错误。
    """

    def loadDataFromReginsInfo(self, tableName, reginsInfo, masterIp, port, colnames):
        """
        该函数为按照ReginsInfo来提取数据的函数
        函数为单线程函数
        pyspark可以多线程并行调用，只要保证ReginsInfo不同即可读取不同区块的数据
        :param reginsInfo: Hbase中某个表的某个Regin的相关信息
        :param masterIp: 读取数据的masterIP
        :param port: 读取数据的master的port
        :param colnames: 需要提取的列的列名。例子:["data:title", "data:pngFile"]
        :return: 函数返回list格式的数据，每一个list成员为读取Hbase的result
        """
        # 定义读取的连个参数。读取的key上界和下届
        startKey = reginsInfo.startKey
        endKey = reginsInfo.endKey
        # 建立HBase连接
        transport = TSocket.TSocket(masterIp, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        # print "client ready"
        # 定义扫描的scannerid
        scannerID = client.scannerOpenWithStop(tableName, startKey, endKey, colnames,None)
        # 定义保存批量读取所有数据的数组
        dataList = []
        # 读取数据提取的迭代器
        result = client.scannerGet(scannerID)
        # 遍历迭代器，将结果放入数组dataList中（以下三行全量读取，正式运行时须取消注释）
        # while result:
        #     dataList += result
        #     result = client.scannerGet(scannerID)
        # 下行为仅读取10条的测试代码（正式运行时须注释掉）
        dataList += client.scannerGetList(scannerID, 10)
        transport.close()
        # print "client close"
        return dataList

    def collectDataFromTRegionInfo(self, resultData):
        """
        该函数用于从单行的hbase记录中读取指定列的数据，组合成dictionary返回
        通常用于pyspark的RDD数据解析，转换读取的hbase——result数据为dictionary
        :param resultData: h读取base时得到的result数据
        :return: 以键值对形式返回的dictionary格式的单行数据
        """
        dic = {}
        keys = resultData.columns.keys()
        # 特殊的，单独提取rowKey
        dic[b'rowKey'] = resultData.row
        for key in keys:
            dic[key] = resultData.columns[key].value
        return dic

    def putDataAsPartition(self, data, colNames, tableName, ip, serverPort):
        '''
        该函数为在mapPartation中调用的功能函数。接受的RDD数据以迭代器的形式传入。
        通过遍历迭代器，将迭代器中的数据缓冲到一个缓冲变量中。
        当缓冲变量中的数据量到达1000条时，将数据推送到hbase中，然后清空变量，姐搜下一批数据。
        :param data: 包含数据的迭代器。
        :param colNames: 需要推送的列的列名
        :param tableName: 需要推送的目标表的表名
        :param ip: 推送的目标thrift ip
        :param serverPort: 推送的目标thrift port
        :return: 每一行对应的缓冲变量的索引编号
        '''
        # 建立hbase连接
        transport = TSocket.TSocket(ip, serverPort)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        # 开始收集数据
        result = []
        return_data = []
        count = 0
        for line in data:
            count += 1
            # 收集数据生成BathMutation
            mutations_ = []
            for colName in colNames:
                try:
                    mutations_.append(Mutation(column=bytes(colName,encoding='utf-8'), value=bytes(line[colName],encoding='utf-8')))
                except:
                    print(line)
            result.append(Hbase.BatchMutation(row=line[b"rowKey"], mutations=mutations_))
            return_data.append(count)
            # 每1000条想hbase推送一次数据
            if count is 1000:
                client.mutateRows(tableName, result, None)
                # print len(result)
                result = []
                # print len(result)
        # 推送出缓冲变量中的剩余数据
        if len(result) > 0:
            client.mutateRows(tableName, result, None)
        # 关闭连接
        transport.close()
        return return_data


class HBase_Tool:
    """
    Hbase工具类，用于pyspark处理Hbase。
    """
    masterIp = None
    port = None
    def __init__(self, masterIp, port):
        """
        初始化函数，需要提供连接的HBase的ip和端口
        :param masterIp: 读取的hbase的master的ip
        :param port: 读取的hbase的master的port
        """
        self.masterIp = masterIp
        self.port = port

    def getHBaseTableAsRDD(self, tableName, sc, colNames):
        """
        从HBase中读取数据，并以RDD的形式返回。
        :param tableName: 读取的tablename
        :param sc: SparkContext，用于生成RDD
        :param colNames:  需要提取的列的列名。例子:["data:title", "data:pngFile"]
        :return: Dictionary格式的数据，每条数据包含对应列的键值对
        """
        print("start map partation")
        # 建立HBase连接
        transport = TSocket.TSocket(self.masterIp, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        # print "client ready"
        # 获取Regins信息
        tableRegins = client.getTableRegions(tableName)
        transport.close()
        # 一条regins信息建立一个partition
        reginsNum = len(tableRegins)
        # 每条数据绑定对应的index，按index分割rdd的partition。partitionBy以key为分割参数。完成分割后，去掉数据中的index，将数据格式还原
        regins_RDD = sc.parallelize(tableRegins).zipWithIndex().map(lambda x: (x[1],x[0])).partitionBy(reginsNum,lambda x: x).map(lambda x: x[1])
        regins_RDD.cache()
        print ("regins_RDD count: " + str(regins_RDD.count()))
        print ("regins_RDD number of partitions: " + str(regins_RDD.getNumPartitions()))
        # regins_RDD.first() is TRegionInfo
        # 传递读取的ip和端口
        masterIp = self.masterIp
        port = self.port
        # 每一条regins记录调用一个读取函数，完成读取后，展平rdd中的数组
        data_RDD = regins_RDD.map(lambda x: HBase_Self_tool().loadDataFromReginsInfo(tableName, x, masterIp, port, colNames)).flatMap(lambda x: x)
        # 为每一行提取指定列的数据，生成Dictionary形式的数据
        return_RDD = data_RDD.map(lambda x: HBase_Self_tool().collectDataFromTRegionInfo(x))
        # 返回RDD
        return return_RDD

    def outputOneData(self, tableName, rowKey, mutations, attributes):
        transport = TSocket.TSocket(self.masterIp, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        client.mutateRow(tableName,rowKey,mutations,attributes)
        transport.close()
        return ""

    def outPutRDDToHBase(self, rdd, colNames, tableName):
        '''
        将RDD中的数据推送到Hbase中。推送的并行度取决于RDD的partition数量。
        每个partation中的数据，每隔1000条触发一次批量写入。
        写入的thrift server参数将引用类中的参数
        :param rdd: 包含需要推送的数据的RDD。每条数据为一个dictionary。
        其中，必须包含一个名为rowKey的字符串字段。该字段会被识别为该条记录在hbase中的rowkey。
        其他字段的key必须是 ‘列簇名:列名’ 的形式。
        :param colNames: 需要统一写入的列名。用于从dictionary中提取数据。例子：["data:title", "data:pngFile"]
        :param tableName: 需要写入的表名
        :return: 缓冲池计数器中的数值
        '''
        ip = self.masterIp
        port = self.port
        # 为避免出现重复生成连接器的情况发生，采用按照partation调用推送函数的形式来推送数据
        result_RDD = rdd.mapPartitions(lambda x: HBase_Self_tool().putDataAsPartition(x,colNames,tableName,ip,port))
        return result_RDD

    def createTable(self, colnFamName, tableName):
        transport = TSocket.TSocket(self.masterIp, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        contents = ColumnDescriptor(name=colnFamName, maxVersions=1)
        client.createTable(tableName, [contents])
        transport.close()

# 测试
# from pyspark import *
# from pyspark.sql import *
# # sc = SparkContext("spark://master:7077", "test pyspark hbase tool")
# sc = SparkContext("spark://master:7077","test_py3")
# spark = SparkSession(sc)
# tableName = b"hb_charts"
# masterIp = b"slave1"
# port = 9090
# colnames = [b"data:title", b"data:pngFile"]
# testTableName = b"test"
# mutations = [Mutation(column=b"data:number",value=b"11")]
# rowKey = b"t000001"
# rdd = HBase_Tool(masterIp,port).getHBaseTableAsRDD(tableName,sc,colnames)
# # HBase_Tool(masterIp,port).createTable("data","hb_charts")
# # HBase_Tool(masterIp,port).outputOneData("test",rowKey,mutations,None)
# countNum = rdd.count()
# rdd.repartition(int(countNum/3000) + 1).persist(StorageLevel.DISK_ONLY)
# print (rdd.first())
# print (HBase_Tool(masterIp,port).outPutRDDToHBase(rdd,colnames,b"test").count())