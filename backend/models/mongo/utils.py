# Author: kk.Fang(fkfkbill@gmail.com)

from types import FunctionType
from typing import *
from datetime import date, datetime

import arrow
from mongoengine import DynamicDocument


class BaseDoc(DynamicDocument):
    meta = {
        'abstract': True,
    }

    def from_dict(self,
                  d: dict,
                  iter_if: FunctionType = None,
                  iter_by: FunctionType = None,
                  ) -> NoReturn:
        """update a record by given dict,
        with an iter function(mostly a lambda) to judge whether applies the change"""
        for k, v in d.items():
            if k not in self.to_mongo().keys():
                # warn develop
                print("warning: a key not in the doc is inserting into mongo.")
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
        for k, v in self.to_mongo().items():
            if k in ():
                continue
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            if k in ("_id", "id"):
                v = str(v)
            d[k] = v
            if datetime_to_str and isinstance(d[k], date):
                d[k] = arrow.get(d[k]).format("YYYY-MM-DD")
            elif datetime_to_str and isinstance(d[k], datetime):
                d[k] = arrow.get(d[k]).format('YYYY-MM-DDTHH:mm:ss')
        return d

