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
        print((smtp_server.split(':')[0], int(smtp_server.split(':')[-1])))
        self.server = smtplib.SMTP(smtp_server.split(':')[0], int(smtp_server.split(':')[-1]))
        self.server.ehlo()
        self.server.starttls()
        self.server.login(fromaddr, password)

        self.msg = MIMEMultipart()
        self.msg['From'] = fromaddr
        self.msg['To'] = COMMASPACE.join(toaddr)
        self.msg['Date'] = formatdate(localtime=True)

    def __del__(self):
        """
        close server
        :return:
        """
        self.server.quit()

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


class NotifyThread(threading.Thread):

    """
    用于监控推送程序情况的线程
    """

    def run(self):
        time.sleep(10*60)

        # 每天12:00发送一个总结性的报告
        def general_report():
            mongo = MongodbControl.MongodbControl()
            mongo_count = mongo.collection.find().count()
            mongo_update = str(mongo.collection.find().sort('last_updated').limit(1).next()['last_updated'])
            with open('mongodb:hb_charts.txt') as f:
                mongo_transfer_update = eval(f.readlines()[0])['update']
                mongo_transfer_datetime = eval(f.readlines()[0])['date']

            sql = MySQLControl.MySQLControl()
            cursor = sql.connection.cursor()
            cursor.execute('select count(*) from core_doc.hibor')
            mysql_count = cursor.fetchone()['count(*)']
            cursor.execute('SELECT * FROM core_doc.hibor ORDER BY update_at LIMIT 1;')
            mysql_update = str(cursor.fetchone()['update_at'])

            with open('mongodb:hibor.txt') as f:
                mysql_transfer_update = eval(f.readlines()[0])['update']
                mysql_transfer_datetime = eval(f.readlines()[0])['date']

            message = '-----MongoDB-----\n' \
                      '数据库数据量：{}\n' \
                      '数据库最新时：{}\n' \
                      '推送进度时间：{}\n' \
                      '最后推送时间：{}\n' \
                      '-----MySQL-----\n' \
                      '数据库数据量：{}\n' \
                      '数据库最新时：{}\n' \
                      '推送进度时间：{}\n' \
                      '最后推送时间：{}\n'\
                .format(mongo_count, mongo_update, mongo_transfer_update, mongo_transfer_datetime,
                        mysql_count, mysql_update, mysql_transfer_update, mysql_transfer_datetime )

            email = MyEmail('smtp.163.com:25', 'bristlegrasses@163.com', ['bristlegrasses@163.com'], 'yancheng19930129')
            email.set_subject('服务器情况报告')
            email.set_bodytext(message)
            email.send()

        sched = BlockingScheduler()
        # 在每天凌晨 12：00 更新本地字典
        sched.add_job(general_report, 'cron', hour=12, minute=0)
        sched.start()

        # 一旦出现异常则实时报告
        while True:
            with open('process_mongodb.log') as f1:
                last_lines = self.tail(f1, 1)
            for line in last_lines:
                if '数据更新到最新！' not in line or 'Hbase 已经写入' not in line:
                    general_report()
                    break
            time.sleep(60*60)

    @ staticmethod
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
