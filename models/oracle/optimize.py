from datetime import datetime

from sqlalchemy import Column, String, Integer, Float
from sqlalchemy.dialects.oracle import DATE

from .utils import BaseModel


class AituneResultSummary(BaseModel):
    __tablename__ = "AITUNR_RESULT_SUMMARY"

    aituneid = Column("AITUNEID", Integer, primary_key=True)
    targetname = Column("TARGETNAME", String)
    sql_id = Column("SQL_ID", String)
    sql_text = Column("SQL_TEXT", String)
    before_plan_hash_value = Column("BEFORE_PLAN_HASH_VALUE", Integer)
    before_elapsed_time = Column("BEFORE_ELAPSED_TIME", Integer)
    before_logical_read = Column("BEFORE_LOGICAL_READ", Integer)
    after_plan_hash_value = Column("AFTER_PLAN_HASH_VALUE", Integer)
    after_elapsed_time = Column("AFTER_ELAPSED_TIME", Integer)
    after_logical_read = Column("AFTER_LOGICAL_READ", Integer)
    perfpromotion = Column("PERFPROMOTION", Integer)
    aitunedate = Column("AITUNEDATE", DATE, default=datetime.now)


class AituneSqlExPlan(BaseModel):
    __tablename__ = "AITUNE_SQL_EX_PLAN"

    aituneid = Column("AITUNEID", Integer, primary_key=True)
    targetname = Column("TARGETNAME", String)
    sql_id = Column("SQL_ID", String)
    plan_hash_value = ("PLAN_HASH_VALUE", Integer)
    statement_id = Column("STATEMENT_ID", String)
    parent_id = Column("PARENT_ID", Integer)
    depth = Column("DEPTH", Integer)
    operation = Column("OPERATION", String)
    options = Column("options", String)
    object_owner = Column("OBJECT_OWNER", String)
    object_name = Column("OBJECT_NAME", String)
    cost = Column("COST", Integer)
    bytes = Column("BYTES", Integer)
    flag = Column("FLAG", String)


class AituneResultDetails(BaseModel):
    __tablename__ = "AITUNE_RESULT_DETAILS"

    aituneid = Column("AITUNEID", Integer, primary_key=True)
    targetname = Column("TARGETNAME", String)
    sql_id = Column("SQL_ID", String)
    aisolution = Column("AISOLUTION", String)
    aitunedate = Column("AITUNEDATE", DATE, default=lambda: datetime.now())


class AituneHistSqlStat(BaseModel):
    __tablename__ = "AITUNE_HIST_SQLSTAT"

    targetname = Column("TARGETNAME", String)
    btime = Column("BTIME", DATE, default=lambda: datetime.now(), primary_key=True)
    inst_id = Column("INST_ID", Integer)
    snap_id = Column("SNAP_ID", Integer)
    sql_id = Column("SQL_ID", String)
    hash_value = Column("HASH_VALUE", Integer)
    deltae = Column("DELTAE", Integer)
    ppxs = Column("PPXS", Integer)
    pgets = Column("PGETS", Integer)
    preads = Column("PREADS", Integer)
    pcpu = Column("PCPU", Integer)
    prows = Column("PROWS", Integer)
    pccwait = Column("PCCWAIT", Integer)
    pelap = Column("PELAP", Float)
