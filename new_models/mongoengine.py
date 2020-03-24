# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ABCDocumentMetaclass",
    "ABCTopLevelDocumentMetaclass",
    "BaseDoc"
]

import abc
import json
from typing import NoReturn
from types import FunctionType

import arrow
from bson import ObjectId
from mongoengine.base.datastructures import EmbeddedDocumentList
from mongoengine import Document, EmbeddedDocument, DateTimeField
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass, DocumentMetaclass

from utils import const
from utils import datetime_utils


class ABCTopLevelDocumentMetaclass(TopLevelDocumentMetaclass, abc.ABCMeta):
    """顶级document的元类，Document, BaseDoc请优先使用这个"""
    pass


class ABCDocumentMetaclass(DocumentMetaclass, abc.ABCMeta):
    """普通document的元类，给EmbeddedDocument使用"""
    pass


class BaseDoc(Document):
    """针对mongoengine的基础文档对象"""

    create_time = DateTimeField(default=lambda: datetime_utils.datetime.now())

    meta = {
        'abstract': True,
        "indexes": ["create_time"]
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
                print(f"warning:"
                      f" a key({k}) not in the document model is inserting into mongodb.")
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
        elif isinstance(recurse, EmbeddedDocument):
            items = {f: getattr(recurse, f, None) for f in recurse._fields}.items()
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
            if isinstance(v, EmbeddedDocumentList):
                v = [self.to_dict(iter_if, iter_by, recurse=a_v) for a_v in v]
            if isinstance(v, EmbeddedDocument):
                v = self.to_dict(iter_if, iter_by, recurse={
                    f: getattr(v, f, None) for f in v._fields})
            d[k] = v
            if datetime_to_str and isinstance(d[k], datetime_utils.datetime):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATETIME_FORMAT)
            elif datetime_to_str and isinstance(d[k], datetime_utils.date):
                d[k] = arrow.get(d[k]).format(const.COMMON_DATE_FORMAT)
        return d

    def __str__(self):
        return json.dumps(self.to_dict())
