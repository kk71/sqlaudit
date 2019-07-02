# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE, CLOB

from .utils import BaseModel


class WorkList(BaseModel):
    __tablename__ = "T_WORK_LIST"

    work_list_id = Column("WORK_LIST_ID", Integer, Sequence('SEQ_WORK_LIST'), primary_key=True)
    work_list_type = Column("WORK_LIST_TYPE", Integer)
    cmdb_id = Column("CMDB_ID", Integer)
    schema_name = Column("SCHEMA_NAME", String)
    task_name = Column("TASK_NAME", String)
    system_name = Column("SYSTEM_NAME", String)
    database_name = Column("DATABASE_NAME", String)
    sql_counts = Column("SQL_COUNTS", Integer)
    submit_date = Column("SUBMIT_DATE", DATE, default=datetime.now)
    submit_owner = Column("SUBMIT_OWNER", String, comment="发起人")
    audit_date = Column("AUDIT_DATE", DATE)
    work_list_status = Column("WORK_LIST_STATUS", Integer, default=0)
    audit_owner = Column("AUDIT_OWNER", String, comment="审批人")
    audit_comments = Column("AUDIT_COMMENTS", String)
    online_date = Column("ONLINE_DATE", DATE)


class SubWorkList(BaseModel):
    __tablename__ = "T_SUB_WORK_LIST"

    work_list_id = Column("WORK_LIST_ID", Integer)
    statement_id = Column("STATEMENT_ID", String)
    static_check_results = Column("STATIC_CHECK_RESULTS", String)
    dynamic_check_results = Column("DYNAMIC_CHECK_RESULTS", String)
    check_time = Column("CHECK_TIME", DATE, default=datetime.now)
    check_owner = Column("CHECK_OWNER", String, comment="实际审批人")
    check_status = Column("CHECK_STATUS", Boolean)
    online_date = Column("ONLINE_DATE", DATE, default=datetime.now)
    online_owner = Column("ONLINE_OWNER", String, comment="上线人")
    elapsed_seconds = Column("ELAPSED_SECONDS", Integer)
    status = Column("STATUS", Boolean)  # 上线是否成功
    error_msg = Column("ERROR_MSG", String)
    comments = Column("COMMENTS", String)
    sql_text = Column("SQL_TEXT", CLOB)
    id = Column("ID", Integer, Sequence("SEQ_T_SUB_WORK_LIST"), primary_key=True)


class WorkListAnalyseTemp(BaseModel):
    __tablename__ = "T_WORKLIST_ANALYSE_TEMP"

    id = Column("ID", Integer, Sequence("SEQ_T_WORKLIST_ANALYSE_TEMP"), primary_key=True)
    session_id = Column("SESSION_ID", String, nullable=False)
    sql_text = Column("SQL_TEXT", CLOB)
    comments = Column("COMMENTS", String)
    analyse_date = Column("ANALYSE_DATE", DATE, default=datetime.now)


class OSQLPlan(BaseModel):
    __tablename__ = "T_SQL_PLAN"

    work_list_id = Column("WORK_LIST_ID", Integer)
    statement_id = Column("STATEMENT_ID", String)
    plan_id = Column("PLAN_ID", Integer)
    timestamp = Column("TIMESTAMP", DATE, primary_key=True)
    remarks = Column("REMARKS", String)
    operation = Column("OPERATION", String)
    options = Column("OPTIONS", String)
    object_node = Column("OBJECT_NODE", String)
    object_owner = Column("OBJECT_OWNER", String)
    object_name = Column("OBJECT_NAME", String)
    object_alias = Column("OBJECT_ALIAS", String)
    object_instance = Column("OBJECT_INSTANCE", Integer)
    object_type = Column("OBJECT_TYPE", String)
    optimizer = Column("OPTIMIZER", String)
    search_columns = Column("SEARCH_COLUMNS", Integer)
    id = Column("ID", Integer)
    parent_id = Column("PARENT_ID", Integer)
    depth = Column("DEPTH", Integer)
    position = Column("POSITION", Integer)
    cost = Column("COST", Integer)
    cardinality = Column("CARDINALITY", Integer)
    bytes = Column("BYTES", Integer)
    other_tag = Column("OTHER_TAG", String)
    partition_start = Column("PARTITION_START", String)
    partition_stop = Column("PARTITION_STOP", String)
    partition_id = Column("PARTITION_ID", Integer)
    distribution = Column("DISTRIBUTION", String)
    cpu_cost = Column("CPU_COST", Integer)
    io_cost = Column("IO_COST", Integer)
    temp_space = Column("TEMP_SPACE", Integer)
    access_predicates = Column("ACCESS_PREDICATES", String)
    filter_predicates = Column("FILTER_PREDICATES", String)
    projection = Column("PROJECTION", String)
    time = Column("TIME", Integer)
    qblock_name = Column("QBLOCK_NAME", String)
