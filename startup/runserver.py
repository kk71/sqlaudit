# Author: kk.Fang(fkfkbill@gmail.com)

from tornado.web import Application
import tornado.ioloop
from tornado.log import enable_pretty_logging

import settings
from models import init_models

# initiate database models/connections

init_models()

from restful_api.urls import urls


def main():
    """start a web server for sqlaudit restful_api"""
    print("Starting http server for restful api...")
    app = Application(
        urls,
        debug=settings.DEBUG,
        autoreload=True,
    )
    enable_pretty_logging()
    app.listen(settings.WEB_PORT, settings.WEB_IP)
    print(f"Listening on port {settings.WEB_IP}:{settings.WEB_PORT} ...")
    tornado.ioloop.IOLoop.instance().start()


