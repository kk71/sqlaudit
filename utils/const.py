# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union

# 时间格式

COMMON_DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'
COMMON_DATE_FORMAT = 'YYYY-MM-DD'
COMMON_DATE_FORMAT_COMPACT = 'YYYYMMDD'
COMMON_TIME_FORMAT = 'HH:mm:ss'

# 数据库类型

DB_ORACLE = "oracle"
DB_MYSQL = "mysql"
ALL_SUPPORTED_DB_TYPE = (DB_ORACLE, DB_MYSQL)

# 纳管数据库的任务类型

DB_TASK_CAPTURE = "采集及分析"
DB_TASK_TUNE = "SQL智能优化"
ALL_DB_TASKS = (DB_TASK_CAPTURE, DB_TASK_TUNE)

# 任务执行状态

TASK_NEVER_RAN = 0
TASK_PENDING = 1
TASK_RUNNING = 2
TASK_DONE = 3
TASK_FAILED = 4
ALL_TASK_EXECUTION_STATUS = (
    TASK_NEVER_RAN,
    TASK_PENDING,
    TASK_RUNNING,
    TASK_DONE,
    TASK_FAILED
)
ALL_TASK_EXECUTION_STATUS_CHINESE_MAPPING = {
    TASK_NEVER_RAN: "从未执行",
    TASK_PENDING: "等待中",
    TASK_RUNNING: "正在执行",
    TASK_DONE: "成功",
    TASK_FAILED: "失败"
}

# 业务类型

MODEL_OLTP = "OLTP"
MODEL_OLAP = "OLAP"
ALL_SUPPORTED_MODEL = (MODEL_OLAP, MODEL_OLTP)

# 规则状态

RULE_STATUS_ON = "ON"
RULE_STATUS_OFF = "OFF"
ALL_RULE_STATUS = (RULE_STATUS_ON, RULE_STATUS_OFF)

# 线上规则类型

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
OBJ_RULE_TYPE_SEQ = "SEQUENCE"
ALL_OBJ_RULE_TYPE = (OBJ_RULE_TYPE_TABLE, OBJ_RULE_TYPE_PART_TABLE, OBJ_RULE_TYPE_INDEX,
                     OBJ_RULE_TYPE_VIEW, OBJ_RULE_TYPE_SEQ)

# 仪表盘以及概览页所需的统计数据

STATS_NUM_SQL_TEXT = RULE_TYPE_TEXT
STATS_NUM_SQL_PLAN = RULE_TYPE_SQLPLAN
STATS_NUM_SQL_STATS = RULE_TYPE_SQLSTAT
STATS_NUM_SQL = "SQL"  # 这一条包括了上面的三种的和
STATS_NUM_TAB = OBJ_RULE_TYPE_TABLE
STATS_NUM_INDEX = OBJ_RULE_TYPE_INDEX
STATS_NUM_SEQUENCE = OBJ_RULE_TYPE_SEQ
STATS_NUM_OBJ = RULE_TYPE_OBJ
ALL_STATS_NUM_TYPE = (
    STATS_NUM_SQL_TEXT,
    STATS_NUM_SQL_PLAN,
    STATS_NUM_SQL_STATS,
    STATS_NUM_SQL,
    STATS_NUM_TAB,
    STATS_NUM_INDEX,
    STATS_NUM_SEQUENCE,
    STATS_NUM_OBJ
)

# 所有归类为SQL的规则类型(与OBJ对应)

ALL_RULE_TYPES_FOR_SQL_RULE = [
    RULE_TYPE_TEXT,
    RULE_TYPE_SQLPLAN,
    RULE_TYPE_SQLSTAT,
]

# 定位一条规则的字段们

RULE_ALLOCATING_KEYS = ("db_type", "db_model", "rule_name")

# 线下工单规则类型

TICKET_ANALYSE_TYPE_STATIC = "STATIC"
TICKET_ANALYSE_TYPE_DYNAMIC = "DYNAMIC"
ALL_TICKET_ANALYSE_TYPE = (
    TICKET_ANALYSE_TYPE_STATIC,
    TICKET_ANALYSE_TYPE_DYNAMIC
)

# 静态规则DDL的类型

DDL_TYPE_TABLE = "table"
DDL_TYPE_INDEX = "index"
DDL_TYPE_VIEW = "view"
DDL_TYPE_SEQUENCE = "sequence"
DDL_TYPE_PROCEDURE = "procedure"
DDL_TYPE_TRIGGER = "trigger"
DDL_TYPE_PACKAGE = "package"
DDL_TYPE_JOB = "job"
DDL_TYPE_TYPE = "type"
DDL_TYPE_AUTHORIZATION = "authorization"
ALL_DDL_TYPE = (
    DDL_TYPE_TABLE,
    DDL_TYPE_INDEX,
    DDL_TYPE_VIEW,
    DDL_TYPE_SEQUENCE,
    DDL_TYPE_PROCEDURE,
    DDL_TYPE_TRIGGER,
    DDL_TYPE_PACKAGE,
    DDL_TYPE_JOB,
    DDL_TYPE_TYPE,
    DDL_TYPE_AUTHORIZATION
)

# 任务状态

JOB_STATUS_ERROR = 0
JOB_STATUS_FINISHED = 1
JOB_STATUS_RUNNING = 2
ALL_JOB_STATUS = (JOB_STATUS_ERROR, JOB_STATUS_FINISHED, JOB_STATUS_RUNNING)

# SQL语句的类型

SQL_ANY = None  # 给线下审核工单用
SQL_DML = 0
SQL_DDL = 1
ALL_SQL_TYPE = (SQL_DML, SQL_DDL)
ALL_SQL_TYPE_NAME_MAPPING = {
    SQL_DDL: "DDL",
    SQL_DML: "DML"
}

# SQL语句名称与语句类型的对应关系

SQL_KEYWORDS = {
    SQL_DDL: ["DROP", "CREATE", "ALTER", "TRUNCATE", "REVOKE", "COMMENT", "UNKNOWN"],
    SQL_DML: ["UPDATE", "INSERT", "DELETE", "SELECT", "COMMIT", "ROLLBACK"],
}
ALL_SQL_KEYWORDS = {j for i in SQL_KEYWORDS.values() for j in i}

# 不需要做动态审核的语句类型

SQL_KEYWORDS_NO_DYNAMIC_ANALYSE = ("COMMIT", "ROLLBACK", "GRANT")

# SQL里面的remark是不能被sqlparse当作注释处理的，需要用一个占位符先替换

REMARK_PLACEHOLDER: str = "--REMARKREMARK"

# 评分显示类型

SCORE_BY_AVERAGE = 1  # 按照平均分展示
SCORE_BY_LOWEST = 2  # 按照最低分展示
ALL_SCORE_BY = (SCORE_BY_AVERAGE, SCORE_BY_LOWEST)

# 评分对象名(对应T_OVERVIEW_RATE.ITEM)

OVERVIEW_ITEM_RADAR = "RADAR"
OVERVIEW_ITEM_SCHEMA = "SCHEMA"
ALL_OVERVIEW_ITEM = (OVERVIEW_ITEM_SCHEMA, OVERVIEW_ITEM_RADAR)

# 线下审核工单状态

OFFLINE_TICKET_ANALYSING = 10  # 正在分析
OFFLINE_TICKET_PENDING = 0  # 等待审核
OFFLINE_TICKET_PASSED = 1  # 审核通过
OFFLINE_TICKET_REJECTED = 2  # 审核拒绝
OFFLINE_TICKET_EXECUTED = 3  # 已上线
OFFLINE_TICKET_FAILED = 4  # 上线失败
ALL_OFFLINE_TICKET_STATUS = (
    OFFLINE_TICKET_ANALYSING,
    OFFLINE_TICKET_PENDING,
    OFFLINE_TICKET_PASSED,
    OFFLINE_TICKET_REJECTED,
    OFFLINE_TICKET_EXECUTED,
    OFFLINE_TICKET_FAILED
)
ALL_OFFLINE_TICKET_STATUS_CHINESE = {
    OFFLINE_TICKET_ANALYSING: "正在分析",
    OFFLINE_TICKET_PENDING: "等待审核",
    OFFLINE_TICKET_PASSED: "审核通过",
    OFFLINE_TICKET_REJECTED: "审核拒绝",
    OFFLINE_TICKET_EXECUTED: "已上线",
    OFFLINE_TICKET_FAILED: "上线失败"
}

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

# mail发送时间

ALL_SEND_DATE = ("星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日")
ALL_SEND_TIME = ("0:00", "1:00", "2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00",
                 "9:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00",
                 "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00")

# 排序
SORT_DESC = "desc"
SORT_ASC = "asc"
ALL_SORTS = (SORT_ASC, SORT_DESC)

#规则等级对应的权重(单次扣分)和最大扣分
RULE_LEVEL_SEVERE = "严重",
RULE_LEVEL_WARNING = "告警",
RULE_LEVEL_INFO= "提示"

RULE_LEVEL_SEVERE_MAX_SCORE = 90,
RULE_LEVEL_WARNING_MAX_SCORE = 50,
RULE_LEVEL_INFO_MAX_SCORE = 5

RULE_LEVEL_SEVERE_WEIGHT = 90,
RULE_LEVEL_WARNING_WEIGHT = 25,
RULE_LEVEL_INFO_WEIGHT = 0.1



# 系统权限

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
    # PRIVILEGE_DASHBOARD = (1, TYPE_FE, "仪表盘", "是否允许使用")
    # PRIVILEGE_SQL_HEALTH = (2, TYPE_FE, "SQL健康度", "")
    PRIVILEGE_ONLINE = (3, TYPE_FE, "线上审核", "")
    PRIVILEGE_OFFLINE = (4, TYPE_FE, "线下审核", "")
    PRIVILEGE_SELF_SERVICE_ONLINE = (5, TYPE_BOTH, "自助上线", "")
    PRIVILEGE_SQL_TUNE = (6, TYPE_FE, "智能优化", "")
    PRIVILEGE_USER_MANAGER = (7, TYPE_FE, "用户管理", "")
    PRIVILEGE_CMDB = (8, TYPE_FE, "纳管数据库管理", "")
    PRIVILEGE_TASK = (9, TYPE_FE, "任务管理", "")
    PRIVILEGE_RULE = (10, TYPE_FE, "规则编辑", "")
    PRIVILEGE_SIMPLE_RULE = (11, TYPE_FE, "增加简单规则", "是否允许使用")
    # PRIVILEGE_COMPLEX_RULE = (12, TYPE_FE, "增加复杂规则", "是否允许使用")
    PRIVILEGE_WHITE_LIST = (13, TYPE_FE, "风险白名单管理", "是否允许使用")
    PRIVILEGE_RISK_RULE = (14, TYPE_FE, "风险SQL规则管理", "是否允许使用")
    PRIVILEGE_MAIL_SEND = (15, TYPE_FE, "报告发送管理", "是否允许使用")
    PRIVILEGE_METADATA = (16, TYPE_FE, "元数据", "是否允许使用")
    PRIVILEGE_OFFLINE_TICKET_APPROVAL = (17, TYPE_BOTH, "审批线下工单", "")
    PRIVILEGE_OFFLINE_TICKET_ADMIN = (18, TYPE_BE, "查看全部线下工单", "")
    PRIVILEGE_ROLE_MANAGE = (19, TYPE_FE, "角色管理", "")
    PRIVILEGE_ROLE_USER_MANAGE = (20, TYPE_FE, "用户角色管理", "")
    PRIVILEGE_ROLE_DATA_PRIVILEGE = (21, TYPE_FE, "数据权限配置", "是否允许使用")
    PRIVILEGE_HEALTH_CENTER = (22, TYPE_FE, "健康中心", "")
    PRIVILEGE_TICKET_RULE = (23, TYPE_BOTH, "工单规则", "")

    # 增加了权限之后，记得加入全列表
    ALL_PRIVILEGE = (
        PRIVILEGE_ONLINE,
        PRIVILEGE_OFFLINE,
        PRIVILEGE_SELF_SERVICE_ONLINE,
        PRIVILEGE_SQL_TUNE,
        PRIVILEGE_USER_MANAGER,
        PRIVILEGE_CMDB,
        PRIVILEGE_TASK,
        PRIVILEGE_RULE,
        PRIVILEGE_SIMPLE_RULE,
        PRIVILEGE_WHITE_LIST,
        PRIVILEGE_RISK_RULE,
        PRIVILEGE_MAIL_SEND,
        PRIVILEGE_METADATA,
        PRIVILEGE_OFFLINE_TICKET_APPROVAL,
        PRIVILEGE_OFFLINE_TICKET_ADMIN,
        PRIVILEGE_ROLE_MANAGE,
        PRIVILEGE_ROLE_USER_MANAGE,
        PRIVILEGE_ROLE_DATA_PRIVILEGE,
        PRIVILEGE_HEALTH_CENTER,
        PRIVILEGE_TICKET_RULE
    )

    @classmethod
    def privilege_to_dict(cls, x):
        return dict(zip(cls.NAMES, x))

    @classmethod
    def get_privilege_by_id(cls, privilege_id) -> Union[tuple, None]:
        """
        根据权限id获取权限tuple
        :param privilege_id:
        :return: 注意当权限不存在的时候会返回None，所以务必对返回的东西作判断
        """
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


class TaskLongTimeNoCapturedException(Exception):
    """任务长期未执行成功错误"""
    pass


class CMDBHasNoSchemaBound(Exception):
    """CMDB未绑定任何schema"""
    pass


class NoRiskRuleSetException(Exception):
    """没有设置风险规则"""
    pass


class AdminRequired(Exception):
    """仅限管理员操作"""
    pass


class PrivilegeRequired(Exception):
    """权限不足"""
    pass


class CannotUsePositionArgs(Exception):
    """函数参数必须带key"""
    pass


class RuleCodeInvalidException(Exception):
    """规则代码无法执行，或者返回结果非正常"""
    pass


class TicketAnalyseException(Exception):
    """线下工单分析的时候出错，导致部分子工单不能生成"""
    pass


class RequiredModelNotRunException(Exception):
    """相关的model未被先行执行"""
    pass


del Union
