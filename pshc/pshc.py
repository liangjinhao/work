from pyspark.sql.types import *
import json
import datetime


INPUTFORMATCLASS = "org.apache.hadoop.hbase.mapreduce.TableInputFormat"
INPUTKEYCLASS = "org.apache.hadoop.hbase.io.ImmutableBytesWritable"
INPUTVALUECLASS = "org.apache.hadoop.hbase.client.Result"
INKEYCONV = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
INVALUECONV = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"


OUTPUTFORMATCLASS = "org.apache.hadoop.hbase.mapreduce.TableOutputFormat"
OUTPUTKEYCLASS = "org.apache.hadoop.hbase.io.ImmutableBytesWritable"
OUTPUTVALUECLASS = "org.apache.hadoop.io.Writable"
OUTKEYCONV = "org.apache.spark.examples.pythonconverters.StringToImmutableBytesWritableConverter"
OUTVALUECONV = "org.apache.spark.examples.pythonconverters.StringListToPutConverter"


CONF = {
    "hbase.zookeeper.quorum": "emr-header-2.cluster-55102,emr-header-1.cluster-55102,emr-worker-1.cluster-55102"
}


TYPEDICT = {
    "string": StringType(),
    "boolean": BooleanType(),
    "float": FloatType(),
    "int": IntegerType(),
    "long": LongType(),
    "date": DateType(),
    "datetime": TimestampType()
}


def types_to_string(v, data_type):
    """
    transfer other Types to string
    :param v:
    :param data_type:
    :return:
    """
    if not v:
        value = ''
    elif data_type == 'boolean':
        value = v and 'True' or 'False'
    elif data_type == 'date':
        value = v.strftime("%Y-%m-%d")
    elif data_type == 'datetime':
        value = v.strftime("%Y-%m-%d %H:%M:%S")
    else:
        value = '%s' % v
    return value


def string_to_types(v, data_type):
    """
    transfer string to other Types
    :param v:
    :param data_type:
    :return:
    """
    value = None
    if v:
        try:
            if data_type == 'string':
                value = v
            elif data_type == 'boolean':
                value = bool(v)
            elif data_type == 'float':
                value = float(v)
            elif data_type == 'int':
                value = int(v)
            elif data_type == 'date':
                value = datetime.datetime.strptime(v, '%Y-%m-%d')
            elif data_type == 'datetime':
                value = datetime.datetime.strptime(v, '%Y-%m-%d %H:%M:%S')
            else:
                value = v
        except:
            value = None
    return value


class PSHC(object):
    """
    PySpark Hbase connector class, used to access Hbase in PySpark
    """

    def __init__(self, sc, spark_session, conf=CONF):
        self.conf = conf
        self.sc = sc
        self.sparkSession = spark_session
        self.cache_rdd = {}

    def get_df_from_hbase(self, catelog, cached=True):
        """
        Get dataframe from Hbase
        :param catelog: json , eg:
        {
            "table":{"namespace":"default", "name":"table1"},
            "rowkey":"col0",
            "columns":{
              "col0":{"cf":"rowkey", "col":"key", "type":"string"},
              "col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
              "col3":{"cf":"cf3", "col":"col3", "type":"float"},
              "col4":{"cf":"cf4", "col":"col4", "type":"int"},
              "col5":{"cf":"cf5", "col":"col5", "type":"date"},
              "col7":{"cf":"cf7", "col":"col7", "type":"datetime"}
            }
        }
        :param cached:
        :return: :class:`DataFrame`
        """
        conf = self.conf.copy()
        table = catelog['table']['name']
        conf['hbase.mapreduce.inputtable'] = table
        hbase_rdd = self.sc.newAPIHadoopRDD(INPUTFORMATCLASS, INPUTKEYCLASS, INPUTVALUECLASS, keyConverter=INKEYCONV,
                                            valueConverter=INVALUECONV, conf=conf)

        def hrdd_to_rdd(hbase_rdds):
            new_rdds = []
            for index, rdd in enumerate(hbase_rdds):
                values = rdd[1].split("\n")
                new_value = {}
                for x in values:
                    y = json.loads(x)
                    new_value["%s:%s" % (y['columnFamily'], y['qualifier'])] = y['value']
                new_rdd = {}
                for column, v in catelog['columns'].items():
                    real_v = v['cf'] == 'rowkey' and rdd[0] or new_value.get("%s:%s" % (v['cf'], v['col']), None)
                    new_rdd[column] = string_to_types(real_v, v['type'])
                new_rdds.append(new_rdd)
            return new_rdds

        rdds = hbase_rdd.mapPartitions(hrdd_to_rdd)

        if cached:
            rdds.cache()
            self.cache_rdd[table] = rdds

        df = self.sparkSession.createDataFrame(rdds, self.catelog_to_schema(catelog))

        return df

    def uncache_rdd(self, tablename):
        if tablename in self.cache_rdd:
            self.cache_rdd[tablename].unpersist()
        return True

    def save_df_to_hbase(self, dataframe, catelog):
        """
        Save dataframe to Hbase
        :param dataframe: :class:`DataFrame`
        :param catelog: json, eg:
        {
            "table":{"namespace":"default", "name":"table1"},
            "rowkey":"col0",
            "columns":{
              "col0":{"cf":"rowkey", "col":"key", "type":"string"},
              "col1":{"cf":"cf1", "col":"col1", "type":"boolean"},
              "col3":{"cf":"cf3", "col":"col3", "type":"float"},
              "col4":{"cf":"cf4", "col":"col4", "type":"int"},
              "col5":{"cf":"cf5", "col":"col5", "type":"date"},
              "col7":{"cf":"cf7", "col":"col7", "type":"datetime"}
            }
        }
        :return:
        """

        def f(rdds):
            columns = catelog["columns"]
            rowkey = catelog["rowkey"]
            newrdds = []
            for rdd in rdds:
                rdd_dict = rdd.asDict()
                key = str(rdd_dict[rowkey])
                for k, v in rdd_dict.items():
                    if k in columns and k != rowkey:
                        newrdds.append(
                            (key, [key, columns[k]['cf'], columns[k]['col'], types_to_string(v, columns[k]['type'])]))
            return newrdds

        conf = self.conf.copy()
        table = catelog['table']['name']
        conf.update({
            "mapreduce.outputformat.class": OUTPUTFORMATCLASS,
            "mapreduce.job.output.key.class": OUTPUTKEYCLASS,
            "mapreduce.job.output.value.class": OUTPUTVALUECLASS
        })
        conf['hbase.mapred.outputtable'] = table
        dataframe.rdd.mapPartitions(f).saveAsNewAPIHadoopDataset(conf=conf, keyConverter=OUTKEYCONV,
                                                                 valueConverter=OUTVALUECONV)
        return True

    @staticmethod
    def catelog_to_schema(catelog):
        columns_dict = catelog.get('columns')
        columns = columns_dict.keys()
        structtypelist = [StructField(x, TYPEDICT.get(columns_dict[x]['type'], StringType()), True) for x in columns]
        schema = StructType(structtypelist)
        return schema

    @staticmethod
    def demo():
        """
        spark-submit --master yarn --executor-memory 4G --executor-cores 2 --num-executors 4
        --packages
        org.apache.hbase:hbase:1.1.12,
        org.apache.hbase:hbase-server:1.1.12,
        org.apache.hbase:hbase-client:1.1.12,
        org.apache.hbase:hbase-common:1.1.12
        --jars
        ./spark-examples_2.10-1.6.4-SNAPSHOT.jar
        --conf
        spark.pyspark.python=/path to virtualenv python path
        --py-files
        pshc.py

        :return:
        """

        from pyspark import SparkConf, SparkContext
        from pyspark.sql import SparkSession

        conf = SparkConf().setAppName("test")
        sc = SparkContext(conf=conf)
        spark_session = SparkSession(sc)
        tests = PSHC(sc, spark_session)

        catelog1 = {
            "table": {"namespace": "default", "name": "test1"},
            "rowkey": "id",
            "domain": "",
            "columns": {
                "id": {"cf": "rowkey", "col": "key", "type": "string"},
                "number": {"cf": "info", "col": "number", "type": "int"},
                "message": {"cf": "info", "col": "message", "type": "string"}
            }
        }

        catelog2 = {
            "table": {"namespace": "default", "name": "test2"},
            "rowkey": "id",
            "domain": "",
            "columns": {
                "id": {"cf": "rowkey", "col": "key", "type": "string"},
                "count": {"cf": "info", "col": "count", "type": "int"},
                "information": {"cf": "info", "col": "information", "type": "string"}
            }
        }

        # create dataframe1 and save to Hbase
        df1 = spark_session.createDataFrame([{'id': '1', 'number': 3, 'message': 'message1'},
                                            {'id': '2', 'number': 4, 'message': 'message2'},
                                            {'id': '3', 'number': 5, 'message': 'message3'}])
        tests.save_df_to_hbase(df1, catelog1)

        # create dataframe2 and save to Hbase
        df2 = spark_session.createDataFrame([{'id': '1', 'count': 4, 'information': 'information1'},
                                            {'id': '2', 'count': 5, 'information': 'information2'},
                                            {'id': '3', 'count': 6, }])
        tests.save_df_to_hbase(df2, catelog2)

        df_1 = tests.get_df_from_hbase(catelog1)
        df_1.show()
        df_2 = tests.get_df_from_hbase(catelog2)
        df_2.show()

        df_1.registerTempTable('table_test1')
        df_2.registerTempTable('table_test2')

        new_df = spark_session.sql("SELECT table_test1.id, table_test1.number, table_test1.message "
                                   "FROM table_test1 JOIN table_test2 ON table_test1.id == table_test2.id")
        new_df.show()
        new_df.filter("information IS NOT null AND number > 3")
        new_df.show()
