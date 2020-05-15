# Author: kk.Fang(fkfkbill@gmail.com)

import os
from collections import defaultdict

from tornado.web import StaticFileHandler

import settings
from . import api_doc

dynamic_url_modules = [
    # 需要启用模块URL的模块名

    "auth",
    "cmdb",
    "ticket",
    "oracle_cmdb",
    "task",
    "rule",
    "coupling",
]

urls = [
    # static prefix
    (
        os.path.join(settings.STATIC_PREFIX, "(.*)"),
        StaticFileHandler,
        {"path": settings.STATIC_DIR}
    ),
    # api doc
    ("/apidoc", api_doc.APIDocHandler)
]

# 存放用于展示的url信息，按照group分组
verbose_structured_urls = defaultdict(list)
