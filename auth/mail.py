# Author: kk.Fang(fkfkbill@gmail.com)

from models.sqlalchemy import BaseModel

from sqlalchemy import Column, String, Integer


class MailServer(BaseModel):
    """邮件服务器配置"""
    pass


class SendMailList(BaseModel):
    """需要发送邮件的用户"""
    pass


class SendMailHistory(BaseModel):
    """邮件发送历史"""
    pass
