# Author: kk.Fang(fkfkbill@gmail.com)

# 时间格式

COMMON_DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'
COMMON_DATE_FORMAT = 'YYYY-MM-DD'
COMMON_TIME_FORMAT = 'HH:mm:ss'

# 数据库类型

DB_ORACLE = "oracle"
DB_MYSQL = "mysql"
ALL_SUPPORTED_DB_TYPE = (DB_ORACLE, DB_MYSQL)

# 纳管数据库的任务类型
DB_TASK_CAPTURE = "采集及分析"
DB_TASK_TUNE = "SQL智能优化"
ALL_DB_TASKS = (DB_TASK_CAPTURE, DB_TASK_TUNE)

# 业务类型

MODEL_OLTP = "OLTP"
MODEL_OLAP = "OLAP"
ALL_SUPPORTED_MODEL = (MODEL_OLAP, MODEL_OLTP)

# 规则状态

RULE_STATUS_ON = "ON"
RULE_STATUS_OFF = "OFF"
ALL_RULE_STATUS = (RULE_STATUS_ON, RULE_STATUS_OFF)

# 规则类型

RULE_TYPE_OBJ = "OBJ"
RULE_TYPE_TEXT = "TEXT"
RULE_TYPE_SQLPLAN = "SQLPLAN"
RULE_TYPE_SQLSTAT = "SQLSTAT"
ALL_RULE_TYPE = (RULE_TYPE_OBJ, RULE_TYPE_TEXT, RULE_TYPE_SQLPLAN, RULE_TYPE_SQLSTAT)

# OBJ规则的子类型（obj_info_type）

OBJ_RULE_TYPE_TABLE = "TABLE"
OBJ_RULE_TYPE_PART_TABLE = "PART_TABLE"
OBJ_RULE_TYPE_INDEX = "INDEX"
OBJ_RULE_TYPE_VIEW = "VIEW"
ALL_OBJ_RULE_TYPE = (OBJ_RULE_TYPE_TABLE, OBJ_RULE_TYPE_PART_TABLE, OBJ_RULE_TYPE_INDEX,
                     OBJ_RULE_TYPE_VIEW)

# 定位一条规则的字段们

RULE_ALLOCATING_KEYS = ("db_type", "db_model", "rule_name")

# 任务状态

JOB_STATUS_ERROR = 0
JOB_STATUS_FINISHED = 1
JOB_STATUS_RUNNING = 2
ALL_JOB_STATUS = (JOB_STATUS_ERROR, JOB_STATUS_FINISHED, JOB_STATUS_RUNNING)

# SQL语句的类型

SQL_DML = 0
SQL_DDL = 1
ALL_SQL_TYPE = (SQL_DML, SQL_DDL)


class CMDBNotFoundException(Exception):
    """CMDB未找到错误"""
    pass
