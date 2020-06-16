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
from typing import NoReturn, List, Callable, Optional

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
                float_round: Optional[int] = 4,
                **kwargs) -> dict:
        """
        转换为字典
        :param iter_if:过滤字段
        :param iter_by:替换字段数据
        :param datetime_to_str: 日期时间是否转为文本
        :param recurse: 递归调用的输入参数（通常不建议使用者传入该参数）
        :param float_round: 是否给float类型的值保留精度，默认保留4位
        :param kwargs:
        :return:
        """
        d = {}
        if isinstance(recurse, dict):
            items = recurse.items()
        elif isinstance(recurse, EmbeddedDocument):
            items = {f: getattr(recurse, f, None) for f in recurse._fields}.items()
        else:
            items = {f: getattr(self, f, None) for f in self._fields}.items()
        for k, v in items:
            if k in ("auto_id_0", "_cls"):
                continue
            if callable(iter_if) and not iter_if(k, v):
                continue
            if iter_by:
                v = iter_by(k, v)
            if float_round and isinstance(v, float):
                v = round(v, float_round)
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

    @classmethod
    def insert(cls, objects_to_insert: List["BaseDoc"]):
        try:
            return cls.objects.insert(objects_to_insert)
        except Exception as e:
            print(f"failed when inserting data into mongo: {e}")
            print("now trying to insert one by one"
                  " to find out where the problem is...")
            for i in objects_to_insert:
                try:
                    cls.objects.insert([i])
                except Exception as e:
                    print(i.to_dict())
                    raise e

    @classmethod
    def drop_cmdb_related_data(cls, cmdb_id: int):
        """删除纳管库相关的数据"""
        return cls.filter(cmdb_id=cmdb_id).delete()
