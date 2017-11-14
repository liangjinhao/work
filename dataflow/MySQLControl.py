import pymysql.cursors
import pymysql
import configparser

CONFIG_FILE = "path.conf"


class MySQLControl:

    def __init__(self, start_time=None, start_id=None):

        conf = configparser.ConfigParser()
        conf.read(CONFIG_FILE)

        self.host = conf.get("mysql", "host")
        self.port = int(conf.get("mysql", "port"))
        self.db = conf.get("mysql", "db")
        self.table = conf.get("mysql", "table")
        self.user = conf.get("mysql", "user")
        self.password = conf.get("mysql", "password")

        self.connection = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.password,
                                          db=self.db, charset='utf8', cursorclass=pymysql.cursors.DictCursor)

        self.page_size = 1000  # 每次取数据的数量
        self.start_id = ''  # 每次取一轮数据时的启始id

        if start_time is None:
            sql = "SELECT * FROM " + self.db + "." + self.table + " ORDER BY update_at LIMIT 10;"
            cursor = self.connection.cursor()
            cursor.execute(sql)
            self.start_time = cursor.fetchone()['update_at']
        else:
            self.start_time = start_time

        if start_id is None:
            self.start_id = ''
        else:
            self.start_id = start_id

    def __del__(self):
        self.connection.close()

    def yield_data(self):
        """
        
        :return: 
        """
        while True:
            sql = "SELECT * FROM " + self.db + "." + self.table + " WHERE (update_at = %s AND id > %s) " + \
                  " OR (update_at > %s ) ORDER BY update_at, id LIMIT " + str(self.page_size) + ";"
            cursor = self.connection.cursor()
            cursor.execute(sql, (self.start_time, self.start_id, self.start_time))
            print(sql, (self.start_time, self.start_id, self.start_time))
            for row in cursor:
                self.start_time = row['update_at']
                self.start_id = row['id']
                yield(row)
