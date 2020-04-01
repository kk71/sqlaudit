# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy import Column, String, Integer, Boolean, Sequence
from sqlalchemy.dialects.oracle import DATE, CLOB

from .utils import BaseModel
from utils.datetime_utils import *


# TODO DEPRECATED
class DataHealth(BaseModel):
    __tablename__ = "T_DATA_HEALTH"

    id = Column("ID", String, Sequence("SEQ_DATA_HEALTH"), primary_key=True)
    database_name = Column("DATABASE_NAME", String)
    health_score = Column("HEALTH_SCORE", Integer)
    collect_date = Column("COLLECT_DATE", DATE)


