# Author: kk.Fang(fkfkbill@gmail.com)

from models.sqlalchemy import BaseModel

from sqlalchemy import Column, String, Integer, Boolean


class License(BaseModel):
    """序列号信息"""
    id = Column("id", Integer, primary_key=True)
    license_key = Column("license_key", String)
    license_status = Column("license_status", Boolean, default=True)
    error_msg = Column("error_msg", String)
