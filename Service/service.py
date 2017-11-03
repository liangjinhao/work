import json
import time
import tornado.web
from tornado.options import options

import collector

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
            if len(text) < 120:  # image_title长度一般不超过120
                final_dict = collector_service.collect(text)
                final_result.append(final_dict)
            else:
                final_result.append('{}')
        final_json = json.dumps(final_result, ensure_ascii=False)
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "x-requested-with")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.write(final_json)

        print("[INFO]本次耗时" + str((time.time() - start)*1000) + "ms")

        return

    def post(self):
        self.get()


if __name__ == "__main__":

    collector_service = collector.Collector()
    collector_service.collect(' ')

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
