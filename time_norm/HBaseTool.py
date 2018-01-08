# coding=utf-8
# encoding=utf8　

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from hbase import Hbase
from hbase.Hbase import *
import datetime


class HBaseUtils:

    """
    用于在Hbase相关RDD中计算的函数，封装到一个类中方便管理和调用
    该类仅用于读取pyspark读取hbase时使用。
    以此类作为外部调用来避免出现不能序列化的错误。
    """

    @staticmethod
    def read_regions_data(table_name, regions_info, master_ip, port, col_names, attributes=False):
        """
        该函数为按照RegionsInfo来提取数据的函数
        函数为单线程函数
        pyspark可以多线程并行调用，只要保证RegionsInfo不同即可读取不同区块的数据
        :param table_name:
        :param regions_info: Hbase中某个表的某个Region的相关信息
        :param master_ip: 读取数据的masterIP
        :param port: 读取数据的master的port
        :param col_names: 需要提取的列的列名。例子:["data:title", "data:pngFile"]
        :param attributes: 是否对比时间过滤数据
        :return: 函数返回list格式的数据，每一个list成员为读取Hbase的result
        """
        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')

        # 定义读取的连个参数。读取的key上界和下届
        start_key = regions_info.startKey
        end_key = regions_info.endKey

        # 建立HBase连接
        transport = TSocket.TSocket(master_ip, port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()

        scan = TScan()
        # scan.filterString = bytes("SingleColumnValueFilter('{cf}', '{col}', {opt}, 'binary:{val}', true, true)".format(
        #     cf='data', col='', opt="<=", val=str(datetime.datetime.now())), encoding='utf-8')
        # scanner = client.scannerOpenWithScan(table_name, scan, None)
        # client.scannerGet(scanner)

        # 定义扫描的 scanner_id
        scanner_id = client.scannerOpenWithStop(table_name, start_key, end_key, col_names, attributes=None)

        data_list = []
        # result = client.scannerGet(scanner_id)
        # while result:
        #
        #     # 只读取需要的数据列
        #     if attributes:
        #         print(result)
        #         if b'last_updated' in result and b'years_update_time' in result:
        #             if result[b'last_updated'] == result[b'years_update_time']:
        #                 continue
        #
        #     data_list += result
        #     result = client.scannerGet(scanner_id)

        # 下行为仅读取10条的测试代码（正式运行时须注释掉）
        data_list += client.scannerGetList(scanner_id, 10)

        transport.close()

        return data_list

    @staticmethod
    def convert_to_dict(result_data):
        """
        该函数用于从单行的hbase记录中读取指定列的数据，组合成dictionary返回
        通常用于pyspark的RDD数据解析，转换读取的hbase——result数据为dictionary
        :param result_data: 读取hbase得到的result数据
        :return: 以键值对形式返回的dictionary格式的单行数据
        """
        dic = {}
        keys = result_data.columns.keys()
        # 特殊的，单独提取rowKey
        dic['rowKey'] = str(result_data.row, encoding='utf-8')
        for key in keys:
            dic[str(key, encoding='utf-8')] = str(result_data.columns[key].value, encoding='utf-8')
        return dic

    @staticmethod
    def write_data_to_hbase(data, col_names, table_name, ip, server_port):
        """
        该函数为在mapPartation中调用的功能函数。接受的RDD数据以迭代器的形式传入。
        通过遍历迭代器，将迭代器中的数据缓冲到一个缓冲变量中。
        当缓冲变量中的数据量到达1000条时，将数据推送到hbase中，然后清空变量，姐搜下一批数据。
        :param data: 包含数据的迭代器。
        :param col_names: 需要推送的列的列名
        :param table_name: 需要推送的目标表的表名
        :param ip: 推送的目标thrift ip
        :param server_port: 推送的目标thrift port
        :return: 每一行对应的缓冲变量的索引编号
        """
        print("start putDataAsPartition")
        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')
        col_names = HBaseUtils().str_list_to_bytes_list(col_names)

        # 建立hbase连接
        transport = TSocket.TSocket(ip, server_port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()

        # 开始收集数据
        result = []
        count = 0
        for line in data:
            # print("data: " + str(line))
            count += 1
            # 收集数据生成BathMutation
            mutations_ = []
            for colName in col_names:
                if str(colName, encoding='utf-8') in line:
                    mutations_.append(Mutation(column=colName,
                                               value=bytes(line[str(colName, encoding='utf-8')], encoding='utf-8')))
            result.append(Hbase.BatchMutation(row=bytes(line["rowKey"], encoding='utf-8'), mutations=mutations_))
            # 每1000条想hbase推送一次数据
            if count is 1000:
                client.mutateRows(table_name, result, None)
                result = []

        # 推送出缓冲变量中的剩余数据
        if len(result) > 0:
            client.mutateRows(table_name, result, None)

        transport.close()

    @staticmethod
    def str_list_to_bytes_list(str_list):
        """
        用于转换str格式的list为Bytes格式的list
        由于HBase只接受Bytes格式的数据，因此需要做转换处理
        :param str_list:
        :return:
        """
        res = [bytes(string, encoding='utf-8') if not isinstance(string, bytes) else string for string in str_list]
        return res


class HBaseTool:
    """
    Hbase工具类，用于pyspark处理Hbase。
    """
    master_ip = None
    port = None

    def __init__(self, master_ip, port):
        """
        初始化函数，需要提供连接的HBase的ip和端口
        :param master_ip: 读取的hbase的master的ip
        :param port: 读取的hbase的master的port
        """
        # 校验处理字符格式，HBase只接受bytes格式的字符串
        if not isinstance(master_ip, bytes):
            master_ip = bytes(master_ip, encoding='utf-8')

        self.master_ip = master_ip
        self.port = port

    def get_rdd(self, table_name, sc, col_names):
        """
        从HBase中读取数据，并以RDD的形式返回。
        :param table_name: 读取的table_name
        :param sc: SparkContext，用于生成RDD
        :param col_names:  需要提取的列的列名。例子:["data:title", "data:pngFile"]
        :return: Dictionary格式的数据，每条数据包含对应列的键值对
        """
        # 校验处理字符格式，HBase只接受bytes格式的字符串
        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')
        col_names = HBaseUtils().str_list_to_bytes_list(col_names)

        print("start map partation")
        # 建立HBase连接
        transport = TSocket.TSocket(self.master_ip, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()

        # 获取regions信息
        table_regions = client.getTableRegions(table_name)
        transport.close()

        # 一条region信息建立一个partition
        regions_num = len(table_regions)
        # 每条数据绑定对应的index，按index分割rdd的partition。partitionBy以key为分割参数。完成分割后，去掉数据中的index，将数据格式还原
        regions_rdd = sc.parallelize(table_regions).zipWithIndex().map(lambda x: (x[1], x[0]))\
            .partitionBy(regions_num, lambda x: x).map(lambda x: x[1])
        regions_rdd.cache()

        print("regions_rdd count: " + str(regions_rdd.count()))
        print("regions_rdd number of partitions: " + str(regions_rdd.getNumPartitions()))

        # regions_rdd.first() is TRegionInfo

        # 每一条region记录调用一个读取函数，完成读取后，展平rdd中的数组
        data_rdd = regions_rdd.map(
            lambda x: HBaseUtils().read_regions_data(table_name, x, self.master_ip, self.port, col_names,
                                                     attributes=True)).flatMap(lambda x: x)
        # 为每一行提取指定列的数据，生成Dictionary形式的数据
        return_rdd = data_rdd.map(lambda x: HBaseUtils().convert_to_dict(x))
        # 返回RDD
        return return_rdd

    def get_rdd_single_thread(self, table_name, sc, col_names):
        """
        从HBase中读取数据，并以RDD的形式返回。
        RDD约每5万条做一个分区，最终每个分区的数据会少于5万条或接近5万条
        该方法为单线程方法.
        :param table_name: 读取的table_name
        :param sc: SparkContext，用于生成RDD
        :param col_names:  需要提取的列的列名。例子:["data:title", "data:pngFile"]
        :return: Dictionary格式的数据，每条数据包含对应列的键值对
        """
        # 校验处理字符格式，HBase只接受bytes格式的字符串
        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')
        col_names = HBaseUtils().str_list_to_bytes_list(col_names)

        # 建立HBase连接
        transport = TSocket.TSocket(self.master_ip, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        table_regions = client.getTableRegions(table_name)
        count = 0
        data_list = []
        # 遍历region，每个region都会做读取遍历
        for region in table_regions:
            # 定义读取的连个参数。读取的key上界和下届
            start_key = region.startKey
            end_key = region.endKey
            # 定义扫描的scanner_id
            scanner_id = client.scannerOpenWithStop(table_name, start_key, end_key, col_names, None)
            # 定义保存批量读取所有数据的数组
            data_list = []
            # 读取数据提取的迭代器
            result = client.scannerGet(scanner_id)
            # 遍历迭代器，将结果放入数组dataList中
            while result:
                data_list += result
                data_list = client.scannerGet(scanner_id)
                count += 1
                if count % 1000 is 0:
                    print(count)
        transport.close()
        # 用读取的数据的列表生成RDD
        data_rdd = sc.parallelize(data_list).repartation(int(count/50000) + 1)
        return_rdd = data_rdd.map(lambda x: HBaseUtils().convert_to_dict(x))
        return return_rdd

    def write_hbase_mutations(self, table_name, row_key, mutations, attributes):
        """
        向HBase写入一条数据
        :param table_name: 目标表名
        :param row_key: row_key
        :param mutations: 数据内容
        :param attributes: attributes
        :return: 空
        """
        # 校验处理字符格式，HBase只接受bytes格式的字符串
        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')

        transport = TSocket.TSocket(self.master_ip, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        client.mutateRow(table_name, row_key, mutations, attributes)
        transport.close()

    def write_rdd_to_hbase(self, rdd, col_names, table_name):
        """
        将RDD中的数据推送到Hbase中。推送的并行度取决于RDD的partition数量。
        每个partation中的数据，每隔1000条触发一次批量写入。
        写入的thrift server参数将引用类中的参数
        :param rdd: 包含需要推送的数据的RDD。每条数据为一个dictionary。
        其中，必须包含一个名为rowKey的字符串字段。该字段会被识别为该条记录在hbase中的rowkey。
        其他字段的key必须是 ‘列簇名:列名’ 的形式。
        :param col_names: 需要统一写入的列名。用于从dictionary中提取数据。例子：["data:title", "data:pngFile"]
        :param table_name: 需要写入的表名
        :return: 缓冲池计数器中的数值
        """
        # 校验处理字符格式，HBase只接受bytes格式的字符串
        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')
        col_names = HBaseUtils().str_list_to_bytes_list(col_names)

        # 为避免出现重复生成连接器的情况发生，采用按照partition调用推送函数的形式来推送数据
        result_rdd = rdd.mapPartitions(lambda x: HBaseUtils().write_data_to_hbase(x, col_names, table_name,
                                                                                  self.master_ip, self.port))
        return result_rdd

    def create_table(self, col_family, table_name):
        """
        在HBase中建立一个新表
        :param col_family: 列簇名
        :param table_name: 表名
        :return:
        """
        # 校验处理字符格式，HBase只接受bytes格式的字符串
        if not isinstance(table_name, bytes):
            table_name = bytes(table_name, encoding='utf-8')
        if not isinstance(col_family, bytes):
            col_family = bytes(col_family, encoding='utf-8')
        # 完成校验
        transport = TSocket.TSocket(self.master_ip, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        contents = ColumnDescriptor(name=col_family, maxVersions=1)
        client.createTable(table_name, [contents])
        transport.close()

    def find_row(self, table_name, column_family, column, column_value):
        """
        查找hbase中的某条数据
        :param table_name:
        :param column_family:
        :param column:
        :param column_value:
        :return:
        """
        transport = TSocket.TSocket(self.master_ip, self.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()

        scan = TScan()
        scan.filterString = bytes("SingleColumnValueFilter('{cf}', '{col}', {opt}, 'binary:{val}', true, true)".format(
            cf=column_family, col=column, opt="=", val=column_value), encoding='utf-8')
        scanner = client.scannerOpenWithScan(bytes(table_name, encoding='utf-8'), scan, None)

        while True:
            r = client.scannerGet(scanner)
            if not r:
                transport.close()
                break
            else:
                res = {}
                for i in r[0].columns.items():
                    res[i[0]] = i[1].value
                yield res


from pyspark import *
from pyspark.sql import *


def sample():
    sc = SparkContext("spark://master:7077", "test_py3")
    tableName = "hb_charts"
    masterIp = "slave1"
    port = 9090
    colnames = ["data:title", "data:pngFile"]
    testTableName = "test"
    mutations = [Mutation(column=b"data:number", value=b"11")]
    rowKey = b"t000001"
    rdd = HBaseTool(masterIp, port).get_rdd(tableName, sc, colnames)
    HBaseTool(masterIp,port).create_table("data", "hb_charts")
    HBaseTool(masterIp,port).write_hbase_mutations("test", rowKey, mutations, None)
    countNum = rdd.count()
    rdd.repartition(int(countNum / 3000) + 1).persist(StorageLevel.DISK_ONLY)
    print(rdd.first())
    print(HBaseTool(masterIp, port).write_rdd_to_hbase(rdd, colnames, b"test").count())
