# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Float

from models.sqlalchemy import BaseModel


class DataHealthUserConfig(BaseModel):
    """数据库评分配置"""
    __tablename__ = "T_DATA_HEALTH_USER_CONFIG"

    cmdb_id = Column("DATABASE_NAME", String, primary_key=True)
    username = Column("USERNAME", String)
    weight = Column("WEIGHT", Float, default=1)
