import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.utils import COMMASPACE, formatdate
from email import encoders
import os
import threading
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import MongodbControl
import MySQLControl
from filelock import FileLock


class MyEmail:
    """
    Use SMTP send emails
    """

    def __init__(self, smtp_server, fromaddr, toaddr, password):
        """
        :param smtp_server: smtp server, like 'smtp.gmail.com:587', 'smtp.163.com:25'
        :param fromaddr: your address
        :param toaddr: email addresses you send to, like ['Mike@gmail.com', 'Lucy@gmail.com']
        :param password: your address password
        """
        self.fromaddr = fromaddr
        self.toaddr = toaddr
        self.server = smtplib.SMTP_SSL(smtp_server, timeout=10)
        self.server.login(fromaddr, password)

        self.msg = MIMEMultipart()
        self.msg['From'] = fromaddr
        self.msg['To'] = COMMASPACE.join(toaddr)
        self.msg['Date'] = formatdate(localtime=True)

    def set_subject(self, subject):
        """
        set the subject of a email
        :param subject:
        :return:
        """
        self.msg['Subject'] = subject

    def set_bodytext(self, bodytext):
        """
        set the body text of a email
        :param bodytext: the text of email body
        :return:
        """
        self.msg.attach(MIMEText(bodytext, 'plain'))

    def add_attachment(self, file_list):
        """
        add attachments
        :param file_list: list of the paths of files with its extension
        :return:
        """
        for file_path in file_list:
            attachment = open(file_path, "rb").read()
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment)
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=('utf-8', '', os.path.basename(file_path)))
            self.msg.attach(part)

    def send(self):
        """
        send a email
        :return:
        """
        text = self.msg.as_string()
        self.server.sendmail(self.fromaddr, self.toaddr, text)

    @staticmethod
    def demo():
        my_email = MyEmail('smtp.163.com:25', 'bristlegrasses@163.com', ['bristlegrasses@163.com'], 'yancheng19930129')
        my_email.set_bodytext('Just a test mail')
        my_email.set_subject('Report')
        my_email.add_attachment(['/home/yancheng/图片/2017-12-29 18-45-37屏幕截图.png'])
        my_email.send()


def tail(f, lines=1, _buffer=4098):
    """Tail a file and get X lines from the end"""
    # place holder for the lines found
    lines_found = []

    # block counter will be multiplied by buffer
    # to get the block size from the end
    block_counter = -1

    # loop until we find X lines
    while len(lines_found) < lines:
        try:
            f.seek(block_counter * _buffer, os.SEEK_END)
        except IOError:  # either file is too small, or too many lines requested
            f.seek(0)
            lines_found = f.readlines()
            break

        lines_found = f.readlines()

        block_counter -= 1

    return lines_found[-lines:]


def general_report(file_hbcharts_lock, file_hibor_lock, log_hbcharts_lock, log_hibor_lock):
    mongo = MongodbControl.MongodbControl()
    mongo_count = mongo.collection.find().count()
    mongo_update = str(mongo.collection.find().sort([('last_updated', -1)]).limit(1).next()['last_updated'])
    with file_hbcharts_lock:
        with open('mongodb:hb_charts.txt') as f:
            line = f.readlines()[0]
            mongo_transfer_update = eval(line)['update']
            mongo_transfer_datetime = eval(line)['date']

    sql = MySQLControl.MySQLControl()
    cursor = sql.connection.cursor()
    cursor.execute('select count(*) from core_doc.hibor')
    mysql_count = cursor.fetchone()['count(*)']
    cursor.execute('SELECT * FROM core_doc.hibor ORDER BY update_at DESC LIMIT 1;')
    mysql_update = str(cursor.fetchone()['update_at'])
    with file_hibor_lock:
        with open('mysql:hibor.txt') as f:
            line = f.readlines()[0]
            mysql_transfer_update = eval(line)['update']
            mysql_transfer_datetime = eval(line)['date']

    with log_hbcharts_lock:
        with open('process_mongodb.log') as f1:
            last_lines = tail(f1, 10)
            process_mongodb = '\n'.join(last_lines)
    with log_hibor_lock:
        with open('process_mysql.log') as f2:
            last_lines = tail(f2, 10)
            process_mysql = '\n'.join(last_lines)

    message = '-----MongoDB-----\n' \
              '数据库数据量：{}\n' \
              '数据库最新时：{}\n' \
              '推送进度时间：{}\n' \
              '最后推送时间：{}\n' \
              '-----MySQL-----\n' \
              '数据库数据量：{}\n' \
              '数据库最新时：{}\n' \
              '推送进度时间：{}\n' \
              '最后推送时间：{}\n' \
              '-----Mongo.log-----\n' \
              '{}' \
              '-----MySQL.log-----\n' \
              '{}' \
        .format(mongo_count, mongo_update, mongo_transfer_update, mongo_transfer_datetime,
                mysql_count, mysql_update, mysql_transfer_update, mysql_transfer_datetime,
                process_mongodb, process_mysql)
    try:
        email = MyEmail('smtp.163.com:994', 'bristlegrasses@163.com', ['bristlegrasses@163.com'], 'yancheng19930129')
        email.set_subject('服务器情况报告')
        email.set_bodytext(message)
        email.send()
    except Exception as e:
        print(e)
        print(message)


class DailyReportThread(threading.Thread):
    """
    用于每天产生报告消息
    """

    def __init__(self, file_hbcharts_lock, file_hibor_lock, log_hbcharts_lock, log_hibor_lock):
        super(DailyReportThread, self).__init__()
        self.file_hbcharts_lock = file_hbcharts_lock
        self.file_hibor_lock = file_hibor_lock
        self.log_hbcharts_lock = log_hbcharts_lock
        self.log_hibor_lock = log_hibor_lock

    def run(self):
        sched = BlockingScheduler()
        # 在每天凌晨 12：00 更新本地字典
        sched.add_job(general_report, 'cron',
                      args=[self.file_hbcharts_lock, self.file_hibor_lock,
                            self.log_hbcharts_lock, self.log_hibor_lock],
                      hour=12, minute=0)
        # sched.add_job(general_report, 'interval',
        #               args=[self.file_hbcharts_lock, self.file_hibor_lock,
        #                     self.log_hbcharts_lock, self.log_hibor_lock],
        #               minutes=5)
        sched.start()


class NotifyThread(threading.Thread):

    """
    用于监控推送程序情况的线程
    """

    def __init__(self, file_hbcharts_lock, file_hibor_lock, log_hbcharts_lock, log_hibor_lock):
        super(NotifyThread, self).__init__()
        self.file_hbcharts_lock = file_hbcharts_lock
        self.file_hibor_lock = file_hibor_lock
        self.log_hbcharts_lock = log_hbcharts_lock
        self.log_hibor_lock = log_hibor_lock

    def run(self):

        # 每天12:00发送一个总结性的报告
        t = DailyReportThread(self.file_hbcharts_lock, self.file_hibor_lock,
                              self.log_hbcharts_lock, self.log_hibor_lock)
        t.start()

        time.sleep(10 * 60)

        # 一旦出现异常则实时报告
        while True:
            flag = False
            with self.log_hbcharts_lock:
                with open('process_mongodb.log') as f1:
                    last_lines = tail(f1, 1)
                for line in last_lines:
                    if '数据更新到最新！' not in line or 'Hbase 已经写入' not in line:
                        flag = True
                        break
            with self.log_hibor_lock:
                with open('process_mysql.log') as f2:
                    last_lines = tail(f2, 1)
                for line in last_lines:
                    if '数据更新到最新！' not in line or 'Hbase 已经写入' not in line:
                        flag = True
                        break
                if flag:
                    general_report(self.file_hbcharts_lock, self.file_hibor_lock,
                                   self.log_hbcharts_lock, self.log_hibor_lock)
            time.sleep(60*60)


def demo():
    a = NotifyThread()
    a.start()
