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
SCORE_BY_LOWEST = 2  # 按照最低分展示
ALL_SCORE_BY = (SCORE_BY_AVERAGE, SCORE_BY_LOWEST)

# 评分对象名(对应T_OVERVIEW_RATE.ITEM)

OVERVIEW_ITEM_RADAR = "RADAR"
OVERVIEW_ITEM_SCHEMA = "SCHEMA"
ALL_OVERVIEW_ITEM = (OVERVIEW_ITEM_SCHEMA, OVERVIEW_ITEM_RADAR)

# 线下审核工单状态

OFFLINE_TICKET_PENDING = 0  # 等待审核
OFFLINE_TICKET_PASSED = 1  # 审核通过
OFFLINE_TICKET_REJECTED = 2  # 审核拒绝
OFFLINE_TICKET_EXECUTED = 3  # 已上线
OFFLINE_TICKET_FAILED = 4  # 上线失败
ALL_OFFLINE_TICKET_STATUS = (
    OFFLINE_TICKET_PENDING,
    OFFLINE_TICKET_PASSED,
    OFFLINE_TICKET_REJECTED,
    OFFLINE_TICKET_EXECUTED,
    OFFLINE_TICKET_FAILED
)

# PARAM_TYPE

PARAM_TYPE_ENV = 4  # 环境
PARAM_TYPE_DATA_CENTER = 1  # 数据中心
ALL_PARAM_TYPE = (PARAM_TYPE_ENV, PARAM_TYPE_DATA_CENTER)

# 风险白名单类型

WHITE_LIST_CATEGORY_USER = 1
WHITE_LIST_CATEGORY_MODULE = 2
WHITE_LIST_CATEGORY_TEXT = 3
ALL_WHITE_LIST_CATEGORY = (WHITE_LIST_CATEGORY_USER,
                           WHITE_LIST_CATEGORY_MODULE,
                           WHITE_LIST_CATEGORY_TEXT)

# 自动优化SQL前后

AI_TUNE_PRE_OPTIMIZED = "B"
AI_TUNE_POST_OPTIMIZED = "A"

# 纳管数据库权限是否算分needcalc
RANKING_CONFIG_NEED_CALC = "Y"
RANKING_CONFIG_NO_CALC = "N"
ALL_RANKING_CONFIG_CALC_OR_NOT = (RANKING_CONFIG_NEED_CALC, RANKING_CONFIG_NO_CALC)


class PRIVILEGE:
    # 权限执行方
    TYPE_BE = 1  # 仅后端处理
    TYPE_FE = 2  # 仅前端处理
    TYPE_BOTH = 3  # 皆处理
    ALL_PRIVILEGE_TYPE = (TYPE_BE, TYPE_FE, TYPE_BOTH)

    # 权限键名
    NAMES = ("id", "type", "name", "description")

    # 权限
    # 新增权限的时候，请保持id不断增大，不要复用旧id，哪怕已经删掉的权限的id也不要用)
    # 删除权限请全代码搜索删除
    PRIVILEGE_DASHBOARD = (1, TYPE_FE, "仪表盘页", "是否允许使用")
    PRIVILEGE_SQL_HEALTH = (2, TYPE_FE, "SQL健康度页", "是否允许使用")
    PRIVILEGE_ONLINE = (3, TYPE_FE, "线上审核页", "是否允许使用")
    PRIVILEGE_OFFLINE = (4, TYPE_FE, "线下审核页", "是否允许使用")
    PRIVILEGE_SELF_SERVICE_ONLINE = (5, TYPE_FE, "自助上线页", "是否允许使用")
    PRIVILEGE_SQL_TUNE = (6, TYPE_FE, "SQL智能优化", "是否允许使用")
    PRIVILEGE_USER_MANAGER = (7, TYPE_FE, "用户管理", "是否允许使用")
    PRIVILEGE_CMDB = (8, TYPE_FE, "纳管数据库管理", "是否允许使用")
    PRIVILEGE_TASK = (9, TYPE_FE, "任务管理", "是否允许使用")
    PRIVILEGE_RULE = (10, TYPE_FE, "规则管理", "是否允许使用")
    PRIVILEGE_SIMPLE_RULE = (11, TYPE_FE, "增加简单规则", "是否允许使用")
    PRIVILEGE_COMPLEX_RULE = (12, TYPE_FE, "增加复杂规则", "是否允许使用")
    PRIVILEGE_WHITE_LIST = (13, TYPE_FE, "白名单管理", "是否允许使用")
    PRIVILEGE_RISK_RULE = (14, TYPE_FE, "风险规则管理", "是否允许使用")
    PRIVILEGE_MAIL_SEND = (15, TYPE_FE, "发送报告", "是否允许使用")
    PRIVILEGE_METADATA = (16, TYPE_FE, "元数据", "是否允许使用")

    # 增加了权限之后，记得加入全列表
    ALL_PRIVILEGE = (
        PRIVILEGE_DASHBOARD,
        PRIVILEGE_SQL_HEALTH,
        PRIVILEGE_ONLINE,
        PRIVILEGE_OFFLINE,
        PRIVILEGE_SELF_SERVICE_ONLINE,
        PRIVILEGE_SQL_TUNE,
        PRIVILEGE_USER_MANAGER,
        PRIVILEGE_CMDB,
        PRIVILEGE_TASK,
        PRIVILEGE_RULE,
        PRIVILEGE_SIMPLE_RULE,
        PRIVILEGE_COMPLEX_RULE,
        PRIVILEGE_WHITE_LIST,
        PRIVILEGE_RISK_RULE,
        PRIVILEGE_MAIL_SEND,
        PRIVILEGE_METADATA
    )

    @classmethod
    def privilege_to_dict(cls, x):
        return dict(zip(cls.NAMES, x))

    @classmethod
    def get_privilege_by_id(cls, privilege_id) -> tuple:
        for i in cls.ALL_PRIVILEGE:
            if i[0] == privilege_id:
                return i

    @classmethod
    def get_privilege_by_type(cls, privilege_type) -> list:
        if not isinstance(privilege_type, (tuple, list)):
            privilege_type = (privilege_type,)
        return [i for i in cls.ALL_PRIVILEGE if i[1] in privilege_type]

    @classmethod
    def get_all_privilege_id(cls) -> [int]:
        return [i[0] for i in cls.ALL_PRIVILEGE]


class CMDBNotFoundException(Exception):
    """CMDB未找到错误"""
    pass


class NoRiskRuleSetException(Exception):
    """没有设置风险规则"""
    pass


class PrivilegeRequired(Exception):
    """权限不足"""
    pass
