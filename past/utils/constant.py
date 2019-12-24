# -*- coding: utf-8 -*-
DML = 0
DDL = 1

worklist_type_static_rule = {
    DDL: ["CHECK_INDEX_ONLINE","CHECK_CREATE_TABEL_NOT_DEFINE_TABLE_SPACE","BAD_JOIN", "DML_ALLDATA", "DML_SORT", "LIKE_UNINDEX", "LONG_TEXT", "SELECT_ANY",
          "TOOMANY_BIND", "TOOMANY_IN_LIST", "TOOMANY_OR", "UNION", "WHERE_FUNC", "WHERE_NOT",
          "CHECK_BITMAP_INDEX", "CHECK_COLUMN_NUMBER", "CHECK_CONCURRENCY_LEVEL",
          "CHECK_DB_LINK_CREATED", "CHECK_INDEX_NOT_DEFINE_TABLE_SPACE", "CHECK_LOB_USING",
          "CHECK_SEQUENCE",
          "CHECK_ADD_COLUMN", "CHECK_DROP_INDEX", "CHECK_DROP_TABLE", "CHECK_DELETE_SEQUENCE",
          "CHECK_TRUNCATE_TABLE", "CHECK_DROP_PARTITION", "CHECK_USING_REVOKE",
          "CHECK_MODIFY_COLUMN", "CHECK_DROP_PRIMARY_KEY", "CHECK_DROP_COLUMN"
          ],
    DML: ['CHECK_DB_LINK_OFFLINE','UNION', 'BAD_JOIN', 'LONG_TEXT', 'WHERE_FUNC', 'TOOMANY_BIND', 'TOOMANY_IN_LIST',
          'SUBQUERY_REP', 'DML_ALLDATA', 'SELECT_ANY', 'WHERE_NOT', 'SUBQUERY_HAVING',
          'DML_SORT', 'TOOMANY_OR', 'LIKE_UNINDEX', 'SUBQUERY_SELECT', 'SUBQUERY_FROM',
          'SUBQUERY_WHERE', 'UNION', 'WHERE_FUNC', 'LONG_TEXT', 'BAD_JOIN', 'TOOMANY_IN_LIST',
          'WHERE_NOT', 'TOOMANY_BIND', 'SUBQUERY_REP', 'SUBQUERY_HAVING', 'LIKE_UNINDEX',
          'DML_SORT', 'SELECT_ANY', 'SUBQUERY_FROM', 'DML_ALLDATA', 'TOOMANY_OR',
          'SUBQUERY_SELECT', 'SUBQUERY_WHERE']
}
worklist_type_dynamic_sqlplan_rule = {
    DML: ["LOOP_IN_TAB_FULL_SCAN", "SQL_INDEX_FAST_FULL_SCAN", "SQL_INDEX_FULL_SCAN", "SQL_INDEX_SKIP_SCAN",
          "SQL_LOOP_NUM", "SQL_MERGE_JOIN_CARTESIAN", "SQL_PARALLEL_FETCH", "SQL_PARTITION_RANGE_ALL",
          "SQL_PARTITION_RANGE_INLIST_OR", "SQL_PARTITION_RANGE_ITERATOR", "SQL_TAB_REL_NUM", "SQL_TABLE_FULL_SCAN",
          "SQL_TO_CHANGE_TYPE", "SQL_VIEW_SCAN"],
    DDL: ["LOOP_IN_TAB_FULL_SCAN", "SQL_INDEX_FAST_FULL_SCAN", "SQL_INDEX_FULL_SCAN", "SQL_INDEX_SKIP_SCAN",
          "SQL_LOOP_NUM", "SQL_MERGE_JOIN_CARTESIAN", "SQL_PARALLEL_FETCH", "SQL_PARTITION_RANGE_ALL",
          "SQL_PARTITION_RANGE_INLIST_OR", "SQL_PARTITION_RANGE_ITERATOR", "SQL_TAB_REL_NUM", "SQL_TABLE_FULL_SCAN",
          "SQL_TO_CHANGE_TYPE", "SQL_VIEW_SCAN"]
}

RULE_COMMANDS = {
    'SQL_INDEX_FAST_FULL_SCAN': "db.@sql@.find({OPERATION: 'INDEX', OPTIONS: 'FAST FULL SCAN', USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})",
    'SQL_INDEX_FULL_SCAN': "db.@sql@.find({OPERATION: 'INDEX', OPTIONS: 'FULL SCAN', USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"OBJECT_NAME\":x.OBJECT_NAME,\"ID\":x.ID,\"COST\":x.COST,\"COUNT\":\"\"});})",
    'SQL_TAB_REL_NUM': "db.@sql@.group( { key:{\"USERNAME\":1,\"record_id\":1,\"SQL_ID\":1,\"PLAN_HASH_VALUE\":1,\"OBJECT_TYPE\":1}, cond:{\"USERNAME\":\"@username@\",\"@etl_date_key@\":\"@etl_date@\",\"OBJECT_TYPE\":\"TABLE\"}, reduce:function(curr,result){ result.count++; }, initial:{count:0} } ).forEach(function(x){db.@tmp1@.save({\"SQL_ID\":x.SQL_ID,\"PLAN_HASH_VALUE\":x.PLAN_HASH_VALUE,\"ID\":x.ID,\"COUNT\":x.count})});db.@tmp1@.find({\"COUNT\":{$gte:@tab_num@}}).forEach(function(y){db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,\"ID\":y.ID,\"COUNT\":y.COUNT,\"COST\":\"\",\"OBJECT_NAME\":\"\"})})",
    'LOOP_IN_TAB_FULL_SCAN': "db.@sql@.find({$or: [{OPERATION: /NESTED LOOP/}, {OPERATION: /FILTER/}], USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({SQL_ID: x.SQL_ID, PLAN_HASH_VALUE: x.PLAN_HASH_VALUE, PARENT_ID: x.ID, USERNAME: x.USERNAME, record_id:x.record_id});});db.@tmp@.find().forEach(function(x){db.sqlplan.find({SQL_ID: x.SQL_ID, PLAN_HASH_VALUE: x.PLAN_HASH_VALUE, PARENT_ID: x.PARENT_ID,USERNAME: x.USERNAME, record_id: x.record_id}).forEach(function(y){db.@tmp1@.save({SQL_ID: y.SQL_ID, PLAN_HASH_VALUE: y.PLAN_HASH_VALUE, OBJECT_NAME: y.OBJECT_NAME, ID: y.ID, PARENT_ID: y.PARENT_ID, OPERATION: y.OPERATION, OPTIONS: y.OPTIONS, USERNAME: y.USERNAME, record_id: y.record_id})});});db.@tmp@.drop();db.@tmp1@.aggregate([{$group:{_id:{PARENT_ID:\"$PARENT_ID\",SQL_ID:\"$SQL_ID\",PLAN_HASH_VALUE:\"$PLAN_HASH_VALUE\"},MAXID: {$max:\"$ID\"}}}]).forEach(function(z){db.sqlplan.find({SQL_ID:z._id.SQL_ID,PLAN_HASH_VALUE:z._id.PLAN_HASH_VALUE,$and:[{ID:z.MAXID},{ID:{$ne:2}}],\"USERNAME\":\"@username@\", record_id: '@record_id@', OPERATION:\"TABLE ACCESS\",OPTIONS:\"FULL\"}).forEach(function(y){if(db.obj_tab_info.findOne({OWNER: y.OBJECT_OWNER, IPADDR: '@ip_addr@', SID: '@sid@', TABLE_NAME: y.OBJECT_NAME,$or: [{\"NUM_ROWS\":{$gt:@table_row_num@}},{\"PHY_SIZE(MB)\":{$gt:@table_phy_size@}}]}))db.@tmp@.save({\"SQL_ID\":y.SQL_ID,\"PLAN_HASH_VALUE\":y.PLAN_HASH_VALUE,\"OBJECT_NAME\":y.OBJECT_NAME,\"ID\":y.ID,\"COST\":y.COST,\"COUNT\":\"\"})});})",
    'SQL_TABLE_FULL_SCAN': "db.@sql@.find({OPERATION: 'TABLE ACCESS', OPTIONS: 'FULL', USERNAME: '@username@', record_id: '@record_id@'}).forEach(function(x){db.@tmp@.save({SQL_ID:x.SQL_ID, PLAN_HASH_VALUE:x.PLAN_HASH_VALUE, OBJECT_NAME:x.OBJECT_NAME, ID:x.ID, COST:x.COST, COUNT:''});})"
}

OTHER_SQL = [
    "ADMINISTER",
    "EXPLAIN",
    "FLASHBACK",
    "LOCK",
    "ALTER",
    "ANALYZE",
    "DROP",
    "COMMENT",
    "RENAME",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "REVOKE",
    "GRANT",
    "AUDIT",
    "NOAUDIT",
    "PURGE",
    "CALL",
    "TRUNCATE"
]

CREATE_DBA_SQL = [
    ["CREATE", "", "", "PROFILE"],
    ["CREATE", "", "", "CONTROLFILE"],
    ["CREATE", "", "", "DATABASE"],
    ["CREATE", "", "", "DISKGROUP"],
    ["CREATE", "", "", "FLASHBACK\s+ARCHIVE"],
    ["CREATE", "", "", "LOCKDOWN\s+PROFILE"],
    ["CREATE", "", "", "OUTLINE"],
    ["CREATE", "", "", "PFILE"],
    ["CREATE", "", "", "PLUGGABLE\s+DATABASE"],
    ["CREATE", "", "", "RESTORE\s+POINT"],
    ["CREATE", "", "", "ROLE"],
    ["CREATE", "", "", "ROLLBACK\s+SEGMENT"],
    ["CREATE", "", "", "SPFILE"],
    ["CREATE", "", "", "TABLESPACE"],
    ["CREATE", "", "", "TABLESPACE\s+SET"],
]

CREATE_APP_SQL = [
    # declare 特殊处理
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "FUNCTION", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "PACKAGE", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "PACKAGE BODY", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "PROCEDURE", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "TRIGGER", "\/"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "TYPE\s+BODY", "\/"],
    ["CREATE", "OR\s+REPLACE", "", "", "ANALYTIC\s+VIEW", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "ATTRIBUTE\s+DIMENSION", ";"],
    ["CREATE", "", "", "", "AUDIT\s+POLICY", ";"],
    ["CREATE", "", "", "", "CLUSTER", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "CONTEXT", ";"],
    ["CREATE", "SHARED|PUBLIC", "", "", "DATABASE\s+LINK", ";"],
    ["CREATE", "", "", "", "DIMENSION", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "DIRECTORY", ";"],
    ["CREATE", "", "", "", "EDITION", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "HIERARCHY", ";"],
    ["CREATE", "UNIQUE|BITMAP", "", "", "INDEX", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "INDEXTYPE", ";"],
    ["CREATE", "", "", "", "INMEMORY\s+JOIN\s+GROUP", ";"],
    ["CREATE", "OR\s+REPLACE", "AND\s+RESOLVE|AND\s+COMPILE", "NOFORCE", "JAVA", ";"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "LIBRARY", ";"],
    ["CREATE", "", "", "", "MATERIALIZED\s+VIEW", ";"],
    ["CREATE", "", "", "", "MATERIALIZED\s+VIEW\s+LOG", ";"],
    ["CREATE", "", "", "", "MATERIALIZED\s+ZONEMAP", ";"],
    ["CREATE", "OR\s+REPLACE", "", "", "OPERATOR", ";"],
    ["CREATE", "", "", "", "SCHEMA", ";"],
    ["CREATE", "", "", "", "SEQUENCE", ";"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "PUBLIC", "SYNONYM", ";"],
    ["CREATE", "GLOBAL TEMPORARY|PRIVATE TEMPORARY|SHARDED|DUPLICATED", "", "", "TABLE", ";"],
    ["CREATE", "OR\s+REPLACE", "EDITIONABLE|NONEDITIONABLE", "", "TYPE", ";"],
    ["CREATE", "", "", "", "USER", ";"],
    ["CREATE", "OR\s+REPLACE", "NO FORCE|FORCE", "EDITIONING|EDITIONABLE EDITIONING|NONEDITIONABLE", "VIEW", ";"],
]

SQLPLUS_SQL = [
    "ACC",
    "ACCEPT",
    "A",
    "APPEND",
    "ARCHIVE LOG",
    "ATTR",
    "ATTRIBUTE",
    "BRE",
    "BREAK",
    "BTI",
    "BTITLE",
    "C",
    "CHANGE",
    "CL",
    "CLEAR",
    "COL",
    "COLUMN",
    "COMP",
    "COMPUTE",
    "CONN",
    "CONNECT",
    "DEF",
    "DEFINE",
    "DEL",
    "DESC",
    "DESCRIBE",
    "DISC",
    "DISCONNECT",
    "ED",
    "EDIT",
    "EXEC",
    "EXECUTE",
    "EXIT",
    "GET",
    "HELP",
    "HIST",
    "HISTORY",
    "HO",
    "HOST",
    "I",
    "INPUT",
    "L",
    "LIST",
    "PASS",
    "PASSWORD",
    "PAU",
    "PAUSE",
    "PRINT",
    "PRO",
    "PROMPT",
    "QUIT",
    "RECOVER",
    "REM",
    "REMARK",
    "REPF",
    "REPFOOTER",
    "REPH",
    "REPHEADER",
    "R",
    "RUN",
    "SAV",
    "SAVE",
    "SHO",
    "SHOW",
    "SHUTDOWN",
    "SPO",
    "SPOOL",
    "STA",
    "START",
    "STARTUP",
    "STORE",
    "TIMI",
    "TIMING",
    "TTI",
    "TTITLE",
    "UNDEF",
    "UNDEFINE",
    "VAR",
    "VARIABLE",
    "WHENEVER\s+OSERROR",
    "WHENEVER\s+SQLERROR",
    "XQUERY"
]
