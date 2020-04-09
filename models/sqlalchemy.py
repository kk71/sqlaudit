# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "make_session",
    "QueryEntity",
    "BaseModel",
    "ABCDeclarativeMeta",
    "sqlalchemy_q"
]

import abc
import json
from contextlib import contextmanager
from typing import *
from types import FunctionType

import arrow
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy import Column, DateTime


# TODO must initiate models first!
from . import Session, base

from sqlalchemy.orm.query import Query as sqlalchemy_q
from utils import const
from utils import datetime_utils


@contextmanager
def make_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"session object {id(session)} has been rolled-back"
              f" because of the following exception: ")
        raise e
    finally:
        session.close()


class QueryEntity(List):

    def __init__(self, *args, **kwargs):
        super(QueryEntity, self).__init__(args)
        self.keys = [i.key for i in self]

    def to_dict(self, v, datetime_to_str: bool = True):
        if datetime_to_str:
            v = [
                datetime_utils.dt_to_str(i)
                if isinstance(i, datetime_utils.datetime)
                else i
                for i in v
            ]
        return dict(zip(self.keys, v))

    @classmethod
    def to_plain_list(cls, v):
        """如果只有单个查询参数，将其展开成为单个list"""
        return [i[0] for i in v]


class ABCDeclarativeMeta(DeclarativeMeta, abc.ABCMeta):
    pass


class BaseModel(base):
    """基础sqlalchemy的表对象"""

    __abstract__ = True

    create_time = Column("create_time",
                         DateTime, default=datetime_utils.datetime.now)

    def from_dict(self,
                  d: dict,
                  iter_if: FunctionType = None,
                  iter_by: FunctionType = None,
                  ) -> NoReturn:
        """update a record by given dict,
        with an iter function(mostly a lambda) to judge whether applies the change"""
        for k, v in d.items():
            if k not in dir(self):
                continue
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            setattr(self, k, v)

    def to_dict(self,
                iter_if: FunctionType = None,
                iter_by: FunctionType = None,
                datetime_to_str: bool = True
                ) -> dict:
        d = {}
        for k in self.__dict__:
            if k in ("_sa_instance_state",):
                continue
            v = getattr(self, k)
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            d[k] = v
            if datetime_to_str and isinstance(d[k], datetime_utils.datetime):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATETIME_FORMAT)
            elif datetime_to_str and isinstance(d[k], datetime_utils.date):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATE_FORMAT)
        return d

    def __str__(self):
        return json.dumps(self.to_dict())
