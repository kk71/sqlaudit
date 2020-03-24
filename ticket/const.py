# Author: kk.Fang(fkfkbill@gmail.com)

# 工单状态

TICKET_ANALYSING = 1  # 正在分析
TICKET_PENDING = 2  # 待审核
TICKET_PASSED = 3  # 审核通过
TICKET_REJECTED = 4  # 审核拒绝
TICKET_EXECUTED = 5  # 已上线
TICKET_FAILED = 6  # 上线失败

ALL_TICKET_STATUS = (
    TICKET_ANALYSING,
    TICKET_PENDING,
    TICKET_PASSED,
    TICKET_REJECTED,
    TICKET_EXECUTED,
    TICKET_FAILED
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
SQL_DML = "DML"
SQL_DDL = "DDL"
ALL_SQL_TYPE = (SQL_DML, SQL_DDL)
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

