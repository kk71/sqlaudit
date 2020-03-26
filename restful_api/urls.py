# Author: kk.Fang(fkfkbill@gmail.com)

import os

from tornado.web import StaticFileHandler

import settings
from .views import *

dynamic_urls = [
    # here serves dynamic urls.
    # use as_vew("relative path to the package")
]

# static prefix
urls = [
    (
        os.path.join(settings.STATIC_PREFIX, "(.*)"),
        StaticFileHandler,
        {"path": settings.STATIC_DIR}
    )
]

# user, authentication, privilege, role and other permissions, licence
urls += [
    (r"/api/user/login", user.AuthHandler),
    (r"/api/user/current", user.CurrentUserHandler),
    (r"/api/user/users", user.UserHandler),
    (r"/api/role", permisson.RoleHandler),
    (r"/api/role/user", permisson.RoleUserHandler),
    (r"/api/role/privilege", permisson.SystemPrivilegeHandler),
    (r"/api/permission/cmdb", permisson.CMDBPermissionHandler),
    (r'/api/product/license', product.ProductLicenseHandler),
    (r'/api/product/version', product.VersionHandler),
]

# rule and risk rules
urls += [
    (r"/api/rule/rules", rules.RuleRepoHandler),
    (r"/api/rule/risk_rules", rules.RiskRuleHandler),
    (r"/api/rule/white_list", rules.WhiteListHandler),
]

# CMDB
urls += [
    (r"/api/cmdb/cmdbs", cmdb.CMDBHandler),
    (r"/api/cmdb/schemas", cmdb.SchemaHandler),
    (r"/api/cmdb/trend", cmdb.CMDBHealthTrendHandler),
    (r"/api/cmdb/aggregation", cmdb.CMDBAggregationHandler),
    (r"/api/cmdb/ranking_config", cmdb.RankingConfigHandler),
]

# overall
urls += [
    # 仪表盘
    (r"/api/dashboard", overall.DashboardHandler),
    (r"/api/dashboard/stats_num_drill_down", overall.StatsNumDrillDownHandler),
    (r"/api/dashboard/notice", overall.NoticeHandler),
    (r"/api/metadata/list", overall.MetadataListHandler),
]

# task
urls += [
    (r"/api/task/tasks", task.TaskHandler),
    (r"/api/task/execution_history", task.TaskExecutionHistoryHandler),
    (r"/api/task/manual_execute", task.TaskManualExecute),
    (r"/api/task/flush_q", task.FlushCeleryQ),
]

# offline
urls += [
    # (r"/api/offline/rule", rule.TicketRuleHandler),
    # (r"/api/offline/rule/code", rule.TicketRuleCodeHandler),
    # (r"/api/offline/sql_upload", ticket.SQLUploadHandler),
    # (r"/api/offline/ticket/outer", ticket.TicketOuterHandler),
    # (r"/api/offline/ticket", ticket.TicketHandler),
    # (r"/api/offline/ticket/export", ticket.TicketExportHandler),
    # (r"/api/offline/sub_ticket", sub_ticket.SubTicketHandler),
    # (r"/api/offline/sub_ticket/sql_plan", sub_ticket.SQLPlanHandler),
    # (r"/api/offline/sub_ticket/export", sub_ticket.SubTicketExportHandler),
    # (r"/api/offline/sub_ticket/rule", sub_ticket.SubTicketRuleHandler),
]

# online audit
urls += [
    (r"/api/online/overview", online.OverviewHandler),
    (r"/api/online/overview/score_by", online.OverviewScoreByHandler),
    (r"/api/online/overview/tablespace", online.TablespaceListHandler),
    (r"/api/online/overview/tablespace_trend", online.TablespaceHistoryHandler),
    (r"/api/online/overview/tablespace_trend_sum", online.TablespaceSumHistoryHandler),
    (r"/api/online/object/rule", online.ObjectRiskRuleHandler),
    (r"/api/online/object", online.ObjectRiskListHandler),
    (r"/api/online/object/export", online.ObjectRiskExportReportHandler),
    (r"/api/online/object/table", online.TableInfoHandler),
    (r"/api/online/sql/rule", online.SQLRiskRuleHandler),
    (r"/api/online/sql", online.SQLRiskListHandler),
    (r"/api/online/sql/detail", online.SQLRiskDetailHandler),
    (r"/api/online/sql/export", online.SQLRiskExportReportHandler),
    (r"/api/online/sql/plan", online.SQLPlanHandler),
]

# health status report
urls += [
    (r"/api/report/online/schema_rate", report.OnlineReportSchemaRate),
    (r"/api/report/online/task", report.OnlineReportTaskHandler),
    (r"/api/report/online/rule_detail", report.OnlineReportRuleDetailHandler),
    (r"/api/report/online/plan_detail", report.OnlineReportSQLPlanHandler),
    (r"/api/report/export/xlsx", report.ExportReportXLSXHandler),
    (r"/api/report/export/html", report.ExportReportHTMLHandler),
    (r"/api/report/export/cmdb/html", report.ExportReportCmdbHTMLHandler),
]

# optimize
urls += [
    (r"/api/optimize/results", optimize.OptimizeResultsHandler),
    (r"/api/optimize/details", optimize.OptimizeDetailsHandler),
    (r"/api/optimize/before_plan", optimize.OptimizeBeforePlanHandler),
    (r"/api/optimize/after_plan", optimize.OptimizeAfterPlanHandler),
    (r"/api/optimize/history_plan", optimize.OptimizeHistoryPlanHandler)
]

# self help online
urls += [
    # (r"/api/self_service/overview", self_service.OverviewHandler),
    # (r"/api/self_service/execute", self_service.ExecuteHandler),
]

# report sending manage
urls += [
    (r"/api/mail/list", report_sending.SendListHandler),
    (r"/api/mail/sender", report_sending.ConfigSenderHandler),
    (r"/api/mail/send", report_sending.SendMailHandler),
    (r"/api/mail/history", report_sending.MailHistory),
]

# === 重构的线下审核 ===

# TODO 未来这块url控制是由package path+自定义路径组成的，代码结构还没完善，暂时先手写

import ticket.restful_api.sub_ticket
import ticket.restful_api.ticket
import ticket.restful_api.temp_script
import oracle_cmdb.ticket.restful_api.sub_ticket
import oracle_cmdb.ticket.restful_api.ticket
import oracle_cmdb.ticket.restful_api.online

urls += [

    # common
    (r"/api/ticket/ticket/archive", ticket.restful_api.ticket.ArchiveHandler),
    (r"/api/ticket/ticket/export", ticket.restful_api.ticket.TicketExportHandler),
    (r"/api/ticket/sub_ticket", ticket.restful_api.sub_ticket.SubTicketHandler),
    (r"/api/ticket/sub_ticket/export", ticket.restful_api.sub_ticket.SubTicketExportHandler),
    (r"/api/ticket/temp_script", ticket.restful_api.temp_script.UploadTempScriptHandler),

    # for oracle
    (r"/api/oracle_cmdb/ticket/ticket", oracle_cmdb.ticket.restful_api.ticket.OracleTicketHandler),
    (r"/api/oracle_cmdb/ticket/sub_ticket/issue", oracle_cmdb.ticket.restful_api.sub_ticket.SubTicketIssueHandler),
    (r"/api/oracle_cmdb/ticket/sub_ticket/sql_plan", oracle_cmdb.ticket.restful_api.sub_ticket.SQLPlanHandler),
    (r"/api/oracle_cmdb/ticket/online/overview", oracle_cmdb.ticket.restful_api.online.OracleTicketOnlineOverviewHandler),
    (r"/api/oracle_cmdb/ticket/online/execute", oracle_cmdb.ticket.restful_api.online.OracleTicketOnlineExecuteHandler),

]
