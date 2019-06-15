# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Integer, Sequence, Boolean, DATE

from .utils import BaseModel


class RiskSQLRule(BaseModel):
    __tablename__ = "T_RISK_SQL_RULE"

    risk_sql_rule_id = Column("RISK_SQL_RULE_ID", Integer,
                              Sequence("SEQ_T_RISK_SQL_RULE"), primary_key=True)
    risk_name = Column("RISK_NAME", String, nullable=False)
    severity = Column("SEVERITY", String)
    optimized_advice = Column("OPTIMIZED_ADVICE", String)
    # the following allocates a rule
    db_type = Column("DB_TYPE", String)
    db_model = Column("DB_MODEL", String)
    rule_name = Column("RULE_NAME", String)
    rule_type = Column("RULE_TYPE", String)  # 不做唯一性考虑，只是冗余字段

    def get_3_key(self) -> tuple:
        return self.db_type, self.db_model, self.rule_name


class WhiteListRules(BaseModel):
    __tablename__ = "WHITE_LIST_RULES"

    id = Column("ID", Integer, Sequence("SEQ_WHITE_LIST_RULES"), primary_key=True)
    cmdb_id = Column("CMDB_ID", Integer)
    rule_name = Column("RULE_NAME", String)
    rule_category = Column("RULE_CATEGORY", Integer)
    rule_text = Column("RULE_TEXT", String)
    status = Column("STATUS", Boolean)
    create_date = Column("CREATE_DATE", DATE)
    creator = Column("CREATOR", String)
    comments = Column("COMMENTS", String)
