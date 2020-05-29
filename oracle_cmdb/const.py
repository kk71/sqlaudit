# Author: kk.Fang(fkfkbill@gmail.com)


# 采集的sql数据是哪天的

ALL_TWO_DAYS_CAPTURE = (
    SQL_TWO_DAYS_CAPTURE_TODAY := "today",  # 当日至采集当时的
    SQL_TWO_DAYS_CAPTURE_YESTERDAY := "yesterday"  # 昨日0点至今日0点的
)

# 对象类型
ALL_ORACLE_OBJECT_TYPES = (
    ORACLE_OBJECT_TYPE_TABLESPACE := "TABLESPACE",
    ORACLE_OBJECT_TYPE_TABLE := "TABLE",
    ORACLE_OBJECT_TYPE_INDEX := "INDEX",
    ORACLE_OBJECT_TYPE_SEQUENCE := "SEQUENCE",
    ORACLE_OBJECT_TYPE_VIEW := "VIEW",
    ORACLE_OBJECT_TYPE_TRIGGER := "TRIGGER",
    ORACLE_OBJECT_TYPE_FUNCTION := "FUNCTION",
    ORACLE_OBJECT_TYPE_PROCEDURE := "PROCEDURE",
    ORACLE_OBJECT_TYPE_DB_LINK := "DB_LINK"
)
