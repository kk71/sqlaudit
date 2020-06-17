# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "MailServer",
    "ReceiveMailInfoList",
    "ReceiveMailHistory"
]

from datetime import datetime

from models.sqlalchemy import BaseModel

from sqlalchemy import Column, String, Integer, Boolean, DateTime


class MailServer(BaseModel):
    """邮件服务器配置"""
    __tablename__ = "mail_server"

    mail_server_id = Column(
        "mail_server_id", Integer, primary_key=True, autoincrement=True)
    mail_server_name = Column("mail_server_name", String)
    user = Column("user", String)
    password = Column("password", String, comment="授权码")
    host = Column("host", String)
    port = Column("port", Integer)
    smtp_ssl = Column("protocol", Boolean,comment="是否开启ssl协议:465,25")
    smtp_skip_login = Column("smtp_skip_login", Boolean,comment="是否跳过登录")
    comments = Column("comments", String)


class ReceiveMailInfoList(BaseModel):
    """接收邮件信息列表"""
    __tablename__ = "receive_mail_info_list"

    receive_mail_id = Column(
        "receive_mail_id", Integer, primary_key=True, autoincrement=True)
    report_item_list = Column("report_item_list", String)
    recipient_list = Column("recipient_list", String)
    send_date = Column("send_date", String)
    send_time = Column("send_time", String)
    last_send_status = Column("last_send_status", Boolean)
    last_send_time = Column("last_send_date", DateTime)
    last_error_msg = Column("last_error_msg", String)



class ReceiveMailHistory(BaseModel):
    """接收邮件历史"""
    __tablename__ = "receive_mail_hist"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    receive_mail_id = Column("receive_mail_id", Integer)
    report_item_list = Column("report_item_list", String)
    recipient_list = Column("recipient_list", String)
    send_status = Column("send_status", Boolean)
    send_time = Column("send_date", DateTime, default=datetime.now)
    error_msg = Column("error_msg", String)
    report_path = Column("report_path", String)

