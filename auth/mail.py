# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "MailServer",
    "SendMailList",
    "SendMailHistory"
]

from datetime import datetime

from models.sqlalchemy import BaseModel

from sqlalchemy import Column, String, Integer, Boolean, DateTime


class MailServer(BaseModel):
    """邮件服务器配置"""
    __tablename__ = "mail_server"

    mail_server_id = Column("mail_server_id", Integer, primary_key=True)
    mail_server_name = Column("mail_server_name", String)
    ip_address = Column("ip_address", String)
    protocol = Column("protocol", String)
    port = Column("port", Integer)
    username = Column("username", String)
    password = Column("password", String)
    status = Column("status", Boolean)
    comments = Column("comments", String)
    use_ssl = Column("use_ssl", Boolean)


class SendMailList(BaseModel):
    """需要发送邮件的用户"""
    __tablename__ = "send_mail_list"

    send_mail_id = Column("send_mail_id", Integer, primary_key=True)
    title = Column("title", String)
    contents = Column("contents", String)
    mail_sender = Column("mail_sender", String)
    send_date = Column("send_date", String)
    send_time = Column("send_time", String)
    send_content_item = Column("send_content_item", String)
    last_send_status = Column("last_send_status", Boolean)
    last_send_date = Column("last_send_date", DateTime, default=datetime.now)
    error_msg = Column("error_msg", String)


class SendMailHistory(BaseModel):
    """邮件发送历史"""
    __tablename__ = "send_mail_hist"

    id = Column("id", Integer, primary_key=True)
    send_mail_list_id = Column("send_mail_list_id", Integer)
    receiver = Column("receiver", String)
    file_path = Column("file_path", String)
    create_time = Column("create_time", DateTime, default=datetime.now)
    status = Column("status", Boolean)
