# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ABCDocumentMetaclass",
    "ABCTopLevelDocumentMetaclass",
    "SelfCollectingTopLevelDocumentMetaclass",
    "BaseDoc",
    "values_dict",
    "mongoengine_qs",
    "Q"
]

import abc
import json
from typing import NoReturn, List, Callable

import arrow
from bson import ObjectId
from mongoengine.base.datastructures import EmbeddedDocumentList
from mongoengine import EmbeddedDocument, DateTimeField, DynamicDocument
# TODO do not delete Document!!!
from mongoengine.base.metaclasses import TopLevelDocumentMetaclass, DocumentMetaclass
from mongoengine import Q, QuerySet as mongoengine_qs

import core.self_collecting_class
from utils import const
from utils import datetime_utils


class ABCTopLevelDocumentMetaclass(TopLevelDocumentMetaclass, abc.ABCMeta):
    pass


class SelfCollectingTopLevelDocumentMetaclass(
        TopLevelDocumentMetaclass,
        core.self_collecting_class.SelfCollectingFrameworkMeta):
    pass


class ABCDocumentMetaclass(DocumentMetaclass, abc.ABCMeta):
    pass


def values_dict(qs: mongoengine_qs, *args) -> List[dict]:
    """查询values_list返回字典形式"""
    return [dict(zip(args, i)) for i in list(qs.values_list(*args))]


class BaseDoc(DynamicDocument):
    """针对mongoengine的基础文档对象"""

    create_time = DateTimeField(default=lambda: datetime_utils.datetime.now())

    meta = {
        'abstract': True,
        "indexes": ["create_time"]
    }

    def from_dict(self,
                  d: dict,
                  iter_if: Callable = None,
                  iter_by: Callable = None,
                  **kwargs) -> NoReturn:
        """
        从一个字典更新当前对象值
        with an iter callable(mostly a lambda) to judge whether applies the change"""
        for k, v in d.items():
            if callable(iter_if) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            if k not in dir(self):  # TODO is it strict?
                # warn develop
                print(f"warning:"
                      f" a key({k}) not in the document model is inserting into mongodb.")
            setattr(self, k, v)

    def to_dict(self,
                iter_if: Callable = None,
                iter_by: Callable = None,
                datetime_to_str: bool = True,
                recurse: dict = None,
                **kwargs) -> dict:
        """转换为字典"""
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
            if callable(iter_if) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            if isinstance(v, ObjectId):
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

    @classmethod
    def filter_enabled(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def filter(cls, *args, **kwargs):
        return cls.objects(*args, **kwargs)

    @classmethod
    def aggregate(cls, *args, **kwargs):
        return cls.objects.aggregate(*args, **kwargs)
