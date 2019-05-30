# Author: kk.Fang(fkfkbill@gmail.com)

from types import FunctionType
from typing import *
from datetime import date, datetime

import arrow
from mongoengine import DynamicDocument, EmbeddedDocument


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
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            if k not in dir(self):  # TODO is it strict?
                # warn develop
                print(f"warning: a key({k}) not in the document model is inserting into mongodb.")
            setattr(self, k, v)

    def to_dict(self,
                iter_if: FunctionType = None,
                iter_by: FunctionType = None,
                datetime_to_str: bool = True,
                recurse: dict = None
                ) -> dict:
        d = {}
        if isinstance(recurse, dict):
            items = recurse.items()
        else:
            items = {f: getattr(self, f, None) for f in self._fields}.items()
        for k, v in items:
            if k in ():
                continue
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            if k in ("_id", "id"):
                v = str(v)
            if isinstance(v, dict):
                v = self.to_dict(iter_if, iter_by, recurse=v)
            if isinstance(v, EmbeddedDocument):
                v = self.to_dict(iter_if, iter_by, recurse={
                    f: getattr(v, f, None) for f in v._fields})
            d[k] = v
            if datetime_to_str and isinstance(d[k], datetime):
                d[k] = arrow.get(d[k]).format('YYYY-MM-DDTHH:mm:ss')
            elif datetime_to_str and isinstance(d[k], date):
                d[k] = arrow.get(d[k]).format("YYYY-MM-DD")
        return d

