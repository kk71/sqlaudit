# Author: kk.Fang(fkfkbill@gmail.com)

import os

from tornado.web import StaticFileHandler

import settings
from .views import *


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
    (r"/api/cmdb/trend", cmdb.CMDBHealthTrendHandler),
    (r"/api/cmdb/aggregation", cmdb.CMDBAggregationHandler),
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
    (r"/api/online/overview", online.OverviewHandler),
    (r"/api/online/object", online.ObjectRiskListHandler),
    (r"/api/online/object/export", online.ObjectRiskReportExportHandler),
    (r"/api/online/object/table", online.TableInfoHandler),
    (r"/api/online/sql", online.SQLRiskListHandler),
    (r"/api/online/sql/detail", online.SQLRiskDetailHandler),
    (r"/api/online/sql/export", online.SQLRiskReportExportHandler),
    (r"/api/online/sql/plan", online.SQLPlanHandler),
]

# health status
urls += [
    (r"/api/report/online/tasks", report.OnlineReportTaskListHandler),
    (r"/api/report/online/task", report.OnlineReportTaskHandler),
    (r"/api/report/online/rule_detail", report.OnlineReportRuleDetailHandler),
    (r"/api/report/online/plan_detail", report.OnlineReportSQLPlanHandler),
    (r"/api/report/export/xlsx", report.ExportReportXLSXHandler),
    (r"/api/report/export/html", report.ExportReportHTMLHandler),
]
