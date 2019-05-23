# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE

from backend.models.oracle.utils import BaseModel


class RiskSQLRule(BaseModel):
    __tablename__ = "T_RISK_SQL_RULE"

    risk_sql_rule_id = Column("RISK_SQL_RULE_ID", Integer,
                              Sequence("SEQ_T_RISK_SQL_RULE"), primary_key=True)
    risk_name = Column("RISK_NAME", String, nullable=False)
    risk_sql_dimension = Column("RISK_SQL_DIMENSION", String)  # this column shouldn't set by user
    severity = Column("SEVERITY", String)
    rule_name = Column("RULE_NAME", String)  # refer to mongodb.rule.rule_name
    optimized_advice = Column("OPTIMIZED_ADVICE", String)


class DataHealth(BaseModel):
    __tablename__ = "T_DATA_HEALTH"

    database_name = Column("DATABASE_NAME", String)
    health_score = Column("HEALTH_SCORE", Integer)
    collect_date = Column("COLLECT_DATE", DATE ,primary_key=True)
