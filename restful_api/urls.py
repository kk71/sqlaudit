# Author: kk.Fang(fkfkbill@gmail.com)

import os

from tornado.web import StaticFileHandler

import settings

dynamic_url_modules = [
    "ticket",
    "oracle_cmdb"
]

urls = [
    # static prefix
    (
        os.path.join(settings.STATIC_PREFIX, "(.*)"),
        StaticFileHandler,
        {"path": settings.STATIC_DIR}
    )
]


# === 重构的线下审核 ===


    # (r"/api/oracle_cmdb/ticket/online/overview", oracle_cmdb.ticket.restful_api.online.OracleTicketOnlineOverviewHandler),
    # (r"/api/oracle_cmdb/ticket/online/execute", oracle_cmdb.ticket.restful_api.online.OracleTicketOnlineExecuteHandler),
#
# ]
