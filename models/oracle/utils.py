# Author: kk.Fang(fkfkbill@gmail.com)

from contextlib import contextmanager
from typing import *
from types import FunctionType

# must initiate models first!
from models import Session, base
from utils.datetime_utils import *
from utils import const


@contextmanager
def make_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"session object {id(session)} has been rolled-back because of the following exception: ")
        raise e
    finally:
        session.close()


class QueryEntity(List):

    def __init__(self, *args, **kwargs):
        super(QueryEntity, self).__init__(args)
        self.keys = [i.key for i in self]

    def to_dict(self, v, datetime_to_str: bool = True):
        if datetime_to_str:
            v = [dt_to_str(i) if isinstance(i, datetime) else i for i in v]
        return dict(zip(self.keys, v))


class BaseModel(base):
    __abstract__ = True

    def from_dict(self,
                  d: dict,
                  iter_if: FunctionType = None,
                  iter_by: FunctionType = None,
                  ) -> NoReturn:
        """update a record by given dict,
        with an iter function(mostly a lambda) to judge whether applies the change"""
        for k, v in d.items():
            if k not in self.__dict__:
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
            if datetime_to_str and isinstance(d[k], datetime):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATETIME_FORMAT)
            elif datetime_to_str and isinstance(d[k], date):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATE_FORMAT)
        return d

    def __str__(self):
        return self.to_dict()
