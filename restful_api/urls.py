# Author: kk.Fang(fkfkbill@gmail.com)

import os
from collections import defaultdict

from tornado.web import StaticFileHandler

import settings

dynamic_url_modules = [
    "auth",
    "cmdb",
    "ticket",
    "oracle_cmdb",
]

urls = [
    # static prefix
    (
        os.path.join(settings.STATIC_PREFIX, "(.*)"),
        StaticFileHandler,
        {"path": settings.STATIC_DIR}
    )
]

# 存放用于展示的url信息，按照group分组
verbose_structured_urls = defaultdict(list)
