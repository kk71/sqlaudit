# Author: kk.Fang(fkfkbill@gmail.com)

from types import FunctionType
from typing import *

from bson import ObjectId
from mongoengine import DynamicDocument, EmbeddedDocument

from utils.datetime_utils import *
from utils import const


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
            if k in ("auto_id_0",):
                continue
            if isinstance(iter_if, FunctionType) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            if k in ("_id", "id") and isinstance(v, ObjectId):
                v = str(v)
            if isinstance(v, dict):
                v = self.to_dict(iter_if, iter_by, recurse=v)
            if isinstance(v, EmbeddedDocument):
                v = self.to_dict(iter_if, iter_by, recurse={
                    f: getattr(v, f, None) for f in v._fields})
            d[k] = v
            if datetime_to_str and isinstance(d[k], datetime):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATETIME_FORMAT)
            elif datetime_to_str and isinstance(d[k], date):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATE_FORMAT)
        return d


class BaseDocRecordID(BaseDoc):

    meta = {
        'abstract': True,
    }

    @classmethod
    def filter_by_exec_hist_id(cls, exec_history_id: str):
        """按照record_id查询"""
        return cls.objects.filter(record_id__startswith=exec_history_id)
