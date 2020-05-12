# Author: kk.Fang(fkfkbill@gmail.com)


# 规则代码的调用入口
# TODO 通常，只增不删

ALL_RULE_ENTRIES = (
    # SQL语句类型相关
    RULE_ENTRY_DDL := "DDL",
    RULE_ENTRY_DML := "DML",

    # 工单相关
    RULE_ENTRY_TICKET := "TICKET",
    RULE_ENTRY_TICKET_STATIC := "TICKET_STATIC",
    RULE_ENTRY_TICKET_STATIC_CMDB_INDEPENDENT := "TICKET_STATIC_CMDB_INDEPENDENT",
    RULE_ENTRY_TICKET_DYNAMIC := "TICKET_DYNAMIC",

    # 线上审核相关
    RULE_ENTRY_ONLINE := "ONLINE",

    RULE_ENTRY_ONLINE_OBJECT := "OBJECT",
    RULE_ENTRY_ONLINE_TABLE := "OBJECT_TABLE",
    RULE_ENTRY_ONLINE_INDEX := "OBJECT_INDEX",
    RULE_ENTRY_ONLINE_SEQUENCE := "OBJECT_SEQUENCE",

    RULE_ENTRY_ONLINE_SQL := "SQL",
    RULE_ENTRY_ONLINE_SQL_TEXT := "SQL_TEXT",
    RULE_ENTRY_ONLINE_SQL_STAT := "SQL_STAT",
    RULE_ENTRY_ONLINE_SQL_PLAN := "SQL_PLAN",

    # 是逐个分析的规则，还是按照某个单位批量分析的
    RULE_ENTRY_ONLINE_SINGLE := "SINGLE",
    RULE_ENTRY_ONLINE_BULK := "BULK"
)


# 规则优先级

ALL_RULE_LEVELS = (
    # 这个数字越大，表示越紧急
    RULE_LEVEL_INFO := 1,
    RULE_LEVEL_WARNING := 2,
    RULE_LEVEL_SEVERE := 3
)
RULE_LEVELS_CHINESE = {
    RULE_LEVEL_INFO: "提示",
    RULE_LEVEL_WARNING: "警告",
    RULE_LEVEL_SEVERE: "严重"
}
RULE_LEVEL_SCORE = {
    # 规则优先级与规则权重/最大扣分的关系
    RULE_LEVEL_INFO: {
        "weight": 0.1,
        "max_score": 5
    },
    RULE_LEVEL_WARNING: {
        "weight": 10,
        "max_score": 20
    },
    RULE_LEVEL_SEVERE: {
        "weight": 30,
        "max_score": 30
    }
}


# 规则输入输出的对象类型校验

ALL_RULE_PARAM_TYPES = (
    RULE_PARAM_TYPE_STR := "STR",
    RULE_PARAM_TYPE_INT := "INT",
    RULE_PARAM_TYPE_FLOAT := "FLOAT",
    RULE_PARAM_TYPE_NUM := "NUM",
    RULE_PARAM_TYPE_LIST := "LIST",
    RULE_PARAM_TYPE_SET := "SET"  # 这个类型实际上是不能用的
)


# 当需要从规则墨盒更新规则到纳管库规则的时候，哪些字段不更新

KEYS_NOT_SYNCHRONIZED_FROM_RULE_CARTRIDGE = (
    "level",
)


# 哪些字段如果两者不一致，询问是否要更新

WARN_KEYS_TO_SYNCHRONIZE_WHEN_DIFFERENT = (
    "input_params",
)
