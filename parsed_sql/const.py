# Author: kk.Fang(fkfkbill@gmail.com)


# SQL语句的类型

SQL_ANY = "ANY"  # 给线下审核工单用
ALL_SQL_TYPE = (
    SQL_DDL := "DDL",
    SQL_DML := "DML"
)
ALL_SQL_TYPE_NAME_MAPPING = {
    SQL_DDL: "DDL",
    SQL_DML: "DML"
}


# SQL里面的remark是不能被sqlparse当作注释处理的，需要用一个占位符先替换

REMARK_PLACEHOLDER: str = "--REMARKREMARK"

# SQL语句名称与语句类型的对应关系

SQL_KEYWORDS = {
    SQL_DDL: ["DROP", "CREATE", "ALTER", "TRUNCATE", "REVOKE", "COMMENT", "UNKNOWN"],
    SQL_DML: ["UPDATE", "INSERT", "DELETE", "SELECT", "COMMIT", "ROLLBACK"]
}
ALL_SQL_KEYWORDS = {j for i in SQL_KEYWORDS.values() for j in i}

