# Author: kk.Fang(fkfkbill@gmail.com)


# 任务种类

ALL_TASK_TYPES = (
    TASK_TYPE_TEST := "TEST",
    TASK_TYPE_CAPTURE := "CAPTURE",
    TASK_TYPE_ORACLE_TICKET_ANALYSE := "ORACLE_TICKET",
    TASK_TYPE_TICKET_EXPORT := "TICKET_EXPORT",
    TASK_TYPE_SUB_TICKET_EXPORT := "SUB_TICKET_EXPORT",
    TASK_TYPE_RISK_RULE_OBJ := "RISK_RULE_OBJ",
    TASK_TYPE_RISK_RULE_SQL := "RISK_RULE_SQL"
)
ALL_TASK_TYPE_CHINESE = {
    TASK_TYPE_TEST: "测试任务",
    TASK_TYPE_CAPTURE: "采集",
    TASK_TYPE_ORACLE_TICKET_ANALYSE: "工单分析",
    TASK_TYPE_TICKET_EXPORT: "工单导出",
    TASK_TYPE_SUB_TICKET_EXPORT: "子工单导出",
    TASK_TYPE_RISK_RULE_OBJ : "风险规则对象导出",
    TASK_TYPE_RISK_RULE_SQL : "风险规则SQL导出"
}


# 任务执行状态

ALL_TASK_EXECUTION_STATUS = (
    TASK_NEVER_RAN := 1,
    TASK_PENDING := 2,
    TASK_RUNNING := 3,
    TASK_DONE := 4,
    TASK_FAILED := 5
)
ALL_TASK_EXECUTION_STATUS_CHINESE_MAPPING = {
    TASK_NEVER_RAN: "从未执行",
    TASK_PENDING: "等待中",
    TASK_RUNNING: "正在执行",
    TASK_DONE: "成功",
    TASK_FAILED: "失败"
}

