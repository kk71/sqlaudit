# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Integer, Sequence, Boolean
from sqlalchemy.dialects.oracle import DATE

from .utils import BaseModel
from utils.datetime_utils import *
from utils import const


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
    __tablename__ = "T_WHITE_LIST_RULES"

    id = Column("ID", Integer, Sequence("SEQ_WHITE_LIST_RULES"), primary_key=True)
    cmdb_id = Column("CMDB_ID", Integer)
    rule_name = Column("RULE_NAME", String)
    rule_category = Column("RULE_CATEGORY", Integer)
    rule_text = Column("RULE_TEXT", String)
    status = Column("STATUS", Boolean)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)
    creator = Column("CREATOR", String)
    comments = Column("COMMENTS", String)

    @classmethod
    def filter_enabled(cls, session, *args, **kwargs):
        return session.query(cls).filter(cls.status == True, *args, **kwargs)


class SendMailList(BaseModel):
    __tablename__="SEND_MAIL_LIST"

    send_mail_id=Column("SEND_MAIL_ID",Integer,Sequence("SEQ_SEND_MAIL_LIST"),primary_key=True)
    title=Column("TITLE",String)
    contents=Column("CONTENTS",String)
    mail_sender=Column("MAIL_SENDER",String)
    send_date=Column("SEND_DATE",String)
    send_time=Column("SEND_TIME",String)
    send_content_item=Column("SEND_CONTENT_ITEM",String)
    last_send_status=Column("LAST_SEND_STATUS",Boolean)
    last_send_date=Column("LAST_SEND_DATE",DATE,default=datetime.now)
    error_msg=Column("ERROR_MSG",String)


class MailServer(BaseModel):
    __tablename__="MAIL_SERVER"

    mail_server_id=Column("MAIL_SERVER_ID",Integer,Sequence("SEQ_MAIL_SERVER"),primary_key=True)
    mail_server_name=Column("MAIL_SERVER_NAME",String)
    ip_address=Column("IP_ADDRESS",String)
    protocol=Column("PROTOCOL",String)
    port=Column("PORT",Integer)
    username=Column("USERNAME",String)
    password=Column("PASSWORD",String)
    status=Column("STATUS",Boolean)
    comments=Column("COMMENTS",String)
    usessl=Column("USESSL",Boolean)


class SendMailHist(BaseModel):
    __tablename__="T_SEND_MAIL_HIST"

    send_mail_list_id=Column("send_mail_list_id",Integer)
    receiver=Column("receiver",String)
    file_path=Column("FILE_PATH",String)
    create_time=Column("CREATE_TIME",DATE,default=datetime.now)
    status=Column("STATUS",Boolean)
    id=Column("ID",Integer,primary_key=True)