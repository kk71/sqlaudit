# Author: kk.Fang(fkfkbill@gmail.com)

import os

from tornado.web import StaticFileHandler

import settings
from backend.views import *


# static prefix
urls = [
    (
        os.path.join(settings.STATIC_PREFIX, "(.*)"),
        StaticFileHandler,
        {"path": settings.STATIC_DIR}
    )
]

# user, authentication, privilege and role
urls += [
    (r"/api/user/login", user.AuthHandler),
    (r"/api/user/users", user.UserHandler),
    (r"/api/role/permission", permisson.SystemPermissionHandler),
]

# rule and risk rules
urls += [
    (r"/api/rule/rules", rules.RuleRepoHandler),
    (r"/api/rule/risk_rules", rules.RiskRuleHandler),
]

# CMDB
urls += [
    (r"/api/cmdb/cmdbs", cmdb.CMDBHandler),
    (r"/api/cmdb/schemas", cmdb.SchemaHandler),
]

# offline audit
urls += [
    (r"/api/offline/ticket", offline.TicketHandler),
    (r"/api/offline/ticket/export", offline.ExportTicketHandler),
    (r"/api/offline/sub_ticket", offline.SubTicketHandler),
    (r"/api/offline/sub_ticket/sql_plan", offline.SubTicketSQLPlanHandler),
    (r"/api/offline/sub_ticket/export", offline.ExportSubTicketHandler),
    (r"/api/offline/sql_upload", offline.SQLUploadHandler),
]

# online audit
urls += [
    (r"/api/online/risk/list", online.RiskListHandler),
    (r"/api/online/risk/detail", online.RiskDetailHandler),
    (r"/api/online/risk/export", online.RiskReportExportHandler),
    (r"/api/online/plan", online.SQLPlanHandler),
]
