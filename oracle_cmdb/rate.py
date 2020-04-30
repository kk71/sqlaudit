# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleRatingSchema"
]

from typing import Callable
from decimal import Decimal

from sqlalchemy import Column, String, Integer, DECIMAL

from models.sqlalchemy import BaseModel


class OracleRatingSchema(BaseModel):
    """数据库评分配置"""
    __tablename__ = "oracle_rating_schema"

    id = Column("id", Integer, primary_key=True, autoincrement=True)
    cmdb_id = Column("cmdb_id", Integer)
    schema_name = Column("schema_name", String)
    weight = Column("weight", DECIMAL, default=1)

    def to_dict(self, *args, decimal_to_float=True, **kwargs) -> dict:
        original_ret = super().to_dict(*args, **kwargs)
        if decimal_to_float:
            original_ret = {
                k: float(v) if isinstance(v, Decimal) else v
                for k, v in original_ret.items()
            }
        return original_ret
