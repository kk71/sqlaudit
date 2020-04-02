# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Float, Integer

from models.sqlalchemy import BaseModel


class OracleRatingSchema(BaseModel):
    """数据库评分配置"""
    __tablename__ = "oracle_rating_schema"

    id = Column("id", primary_key=True)
    cmdb_id = Column("cmdb_id", Integer)
    schema = Column("schema", String)
    weight = Column("weight", Float, default=1)
