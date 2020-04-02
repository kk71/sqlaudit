# Author: kk.Fang(fkfkbill@gmail.com)

from tornado.web import Application
import tornado.ioloop
from tornado.log import enable_pretty_logging

import settings
from models import init_models

# initiate database models/connections

init_models()

import restful_api.modules
import restful_api.urls


def main():
    """start a web server for sqlaudit restful_api"""
    restful_api.modules.collect_dynamic_modules(restful_api.urls.dynamic_url_modules)
    print("starting http server for restful api...")
    app = Application(
        restful_api.urls.urls,
        debug=settings.DEBUG,
        autoreload=True,
    )
    enable_pretty_logging()
    app.listen(settings.WEB_PORT, settings.WEB_IP)
    print(f"listening on port {settings.WEB_IP}:{settings.WEB_PORT} ...")
    tornado.ioloop.IOLoop.instance().start()


