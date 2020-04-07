# Author: kk.Fang(fkfkbill@gmail.com)

# 工单状态

ALL_TICKET_STATUS = (
    TICKET_ANALYSING := 1,  # 正在分析
    TICKET_PENDING := 2,  # 待审核
    TICKET_PASSED := 3,  # 审核通过
    TICKET_REJECTED := 4,  # 审核拒绝
    TICKET_EXECUTED := 5,  # 已上线
    TICKET_FAILED := 6,  # 上线失败
)

ALL_TICKET_STATUS_CHINESE = {
    TICKET_ANALYSING: "正在分析",
    TICKET_PENDING: "待审核",
    TICKET_PASSED: "审核通过",
    TICKET_REJECTED: "审核拒绝",
    TICKET_EXECUTED: "已上线",
    TICKET_FAILED: "上线失败"
}


# SQL里面的remark是不能被sqlparse当作注释处理的，需要用一个占位符先替换

REMARK_PLACEHOLDER: str = "--REMARKREMARK"


# SQL语句的类型

SQL_ANY = "ANY"  # 给线下审核工单用
ALL_SQL_TYPE = (
    SQL_DML := "DML",
    SQL_DDL := "DDL"
)
ALL_SQL_TYPE_NAME_MAPPING = {
    SQL_DDL: "DDL",
    SQL_DML: "DML"
}


# SQL语句名称与语句类型的对应关系

SQL_KEYWORDS = {
    SQL_DDL: ["DROP", "CREATE", "ALTER", "TRUNCATE", "REVOKE", "COMMENT", "UNKNOWN"],
    SQL_DML: ["UPDATE", "INSERT", "DELETE", "SELECT", "COMMIT", "ROLLBACK"]
}
ALL_SQL_KEYWORDS = {j for i in SQL_KEYWORDS.values() for j in i}


# 不需要做动态审核的语句类型

SQL_KEYWORDS_NO_DYNAMIC_ANALYSE = ("COMMIT", "ROLLBACK", "GRANT")


# 子工单展示范围

ALL_SUB_TICKET_FILTERS = (
    SUB_TICKET_WITH_STATIC_PROBLEM := "static",  # 至少包含静态问题
    SUB_TICKET_WITH_DYNAMIC_PROBLEM := "dynamic",  # 至少包含动态问题
    SUB_TICKET_ALL_WITH_PROBLEM := "all_with_problems",  # 包含问题
    SUB_TICKET_HAS_FAILURE := "failure",  # 包含任何阶段的报错
    SUB_TICKET_ALL := "all"  # 全部子工单
)


# 工单规则类型

ALL_TICKET_ANALYSE_TYPE = (
    TICKET_ANALYSE_TYPE_STATIC := "STATIC",
    TICKET_ANALYSE_TYPE_DYNAMIC := "DYNAMIC"
)
