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

# 所有归类为SQL的规则类型(与OBJ对应)
ALL_RULE_TYPES_FOR_SQL_RULE = [
    RULE_TYPE_TEXT,
    RULE_TYPE_SQLPLAN,
    RULE_TYPE_SQLSTAT,
]

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

# 评分显示类型

SCORE_BY_AVERAGE = 1  # 按照平均分展示
SCORE_BY_LOWEST = 2   # 按照最低分展示
ALL_SCORE_BY = (SCORE_BY_AVERAGE, SCORE_BY_LOWEST)

# 评分对象名(对应T_OVERVIEW_RATE.ITEM)

OVERVIEW_ITEM_RADAR = "RADAR"
OVERVIEW_ITEM_SCHEMA = "SCHEMA"
ALL_OVERVIEW_ITEM = (OVERVIEW_ITEM_SCHEMA, OVERVIEW_ITEM_RADAR)

# 线下审核工单状态

OFFLINE_TICKET_PENDING = 0   # 等待审核
OFFLINE_TICKET_PASSED = 1    # 审核通过
OFFLINE_TICKET_REJECTED = 2  # 审核拒绝
OFFLINE_TICKET_EXECUTED = 3  # 已上线
ALL_OFFLINE_TICKET_STATUS = (
    OFFLINE_TICKET_PENDING,
    OFFLINE_TICKET_PASSED,
    OFFLINE_TICKET_REJECTED,
    OFFLINE_TICKET_EXECUTED
)

# PARAM_TYPE

PARAM_TYPE_ENV = 4              # 环境
PARAM_TYPE_DATA_CENTER = 1      # 数据中心
ALL_PARAM_TYPE = (PARAM_TYPE_ENV, PARAM_TYPE_DATA_CENTER)


# 风险白名单类型

WHITE_LIST_CATEGORY_USER = 1
WHITE_LIST_CATEGORY_MODULE = 2
WHITE_LIST_CATEGORY_TEXT = 3
ALL_WHITE_LIST_CATEGORY = (WHITE_LIST_CATEGORY_USER,
                           WHITE_LIST_CATEGORY_MODULE,
                           WHITE_LIST_CATEGORY_TEXT)


class PRIVILEGE:

    # 权限执行方
    TYPE_BE = 1       # 仅后端处理
    TYPE_FE = 2       # 仅前端处理
    TYPE_BOTH = 3     # 皆处理
    ALL_PRIVILEGE_TYPE = (TYPE_BE, TYPE_FE, TYPE_BOTH)

    # 权限类别
    CATEGORY_NORMAL = "通用类型"
    ALL_PRIVILEGE_CATEGORY = (CATEGORY_NORMAL,)

    # 权限键名
    NAMES = ("id", "type", "name", "description", "category")

    # 权限
    PRIVILEGE_DASHBOARD = (1,
                           TYPE_FE,
                           "仪表盘可见", "是否允许使用仪表盘", CATEGORY_NORMAL)
    ALL_PRIVILEGE = (
        PRIVILEGE_DASHBOARD,
    )

    @classmethod
    def privilege_to_dict(cls, x):
        """
        权限列表转为字典
        :param cls:
        :param x:
        :return:
        """
        return dict(zip(cls.NAMES, x))


#自动优化SQL前后
Before_optimize="B"
After_optimize="A"


class CMDBNotFoundException(Exception):
    """CMDB未找到错误"""
    pass


class NoRiskRuleSetException(Exception):
    """没有设置风险规则"""
    pass
