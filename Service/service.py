import json
import time
import tornado.web
from tornado.options import options
import os
import inspect
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import collector
import update_dic
import hanlp_segmentor

tornado.options.define('port', default=8888, help='run on this port', type=int)
tornado.options.define("log_file_prefix", default='tornado_8888.log')
tornado.options.parse_command_line()


class MainHandler(tornado.web.RequestHandler):

    def get(self):
        start = time.time()
        query_text = self.get_argument('text')
        res = json.loads(query_text)

        # 打印信息
        length = len(res)
        print('==========>')
        print("[INFO]开始处理本次请求，有" + str(length) + "条句子")

        # 处理句子
        texts = res
        final_result = []
        for i in range(len(texts)):
            # 清洗数据
            text = texts[i]
            if 0 < len(text) < 120:  # image_title长度一般不超过120
                lock.acquire()
                final_dict = collector_service.collect(text)
                lock.release()
                final_result.append(final_dict)
            else:
                continue
        final_json = json.dumps(final_result, ensure_ascii=False)
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.write(final_json)

        print("[INFO]本次耗时" + str((time.time() - start)*1000) + "ms")

        return

    def post(self):
        self.get()


class WatchFileThread(threading.Thread):
    """
    监测文件改变的线程
    """

    def run(self):
        event_handler = MyHandler()
        observer = Observer()
        directory = os.path.dirname(os.path.abspath(inspect.getsourcefile(lambda: 0)))
        observer.schedule(event_handler, directory, recursive=True)
        observer.start()
        try:
            while True:
                time.sleep(5)
        except Exception as e:
            print(e)
            observer.stop()


class MyHandler(FileSystemEventHandler):

    def on_modified(self, event):
        # /dict/phrase 被修改，重新加载词典
        if event.key[0] == 'modified' and event.key[1].split(r'/')[-1] == 'phrase' and event.key[2] is False:
            lock.acquire()
            collector_service.reload_dict()
            lock.release()
        # /hanlp.properties 被修改，重新加载Hanlp分词词典
        if event.key[0] == 'modified' and 'hanlp' in event.key[1] and event.key[2] is False:
            hanlp_segmentor.HanlpSegmentor().reload_custom_dictionry()


if __name__ == "__main__":
    collector_service = collector.Collector()

    lock = threading.Lock()

    t = WatchFileThread()
    t.start()

    t_d = update_dic.UpdateDictThread()
    t_d.start()

    settings = {
        'template_path': 'views',  # html文件
        'static_path': 'statics',  # 静态文件（css,js,img）
        'static_url_prefix': '/statics/',  # 静态文件前缀
        'cookie_secret': 'adm',  # cookie自定义字符串加盐
    }

    application = tornado.web.Application([(r"/", MainHandler), ], **settings)
    application.listen(options.port)
    print("CRF SERVICE 已经开启！")
    tornado.ioloop.IOLoop.instance().start()
