# Author: kk.Fang(fkfkbill@gmail.com)


# 规则代码的调用入口

# SQL语句类型相关
RULE_ENTRY_DDL = "DDL"
RULE_ENTRY_DML = "DML"

# 工单相关
RULE_ENTRY_TICKET = "TICKET"
RULE_ENTRY_TICKET_STATIC = "TICKET_STATIC"
RULE_ENTRY_TICKET_STATIC_CMDB_INDEPENDENT = "TICKET_STATIC_CMDB_INDEPENDENT"
RULE_ENTRY_TICKET_DYNAMIC = "TICKET_DYNAMIC"

# 线上审核相关
RULE_ENTRY_ONLINE = "ONLINE"

ALL_RULE_ENTRIES = (
    RULE_ENTRY_DDL,
    RULE_ENTRY_DML,
    RULE_ENTRY_TICKET,
    RULE_ENTRY_TICKET_STATIC,
    RULE_ENTRY_TICKET_STATIC_CMDB_INDEPENDENT,
    RULE_ENTRY_TICKET_DYNAMIC,
    RULE_ENTRY_ONLINE
)