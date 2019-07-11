# Author: kk.Fang(fkfkbill@gmail.com)

import re
from types import FunctionType
from typing import *

from bson import ObjectId
from mongoengine import DynamicDocument, EmbeddedDocument, ObjectIdField,\
    IntField, StringField, DateTimeField

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

    @classmethod
    def list_of_values_dict(cls, q, *args):
        """返回dict形式的values"""
        assert args
        q.values_list()
        return


class BaseDocRecordID(BaseDoc):

    meta = {
        'abstract': True,
    }

    @classmethod
    def filter_by_exec_hist_id(cls,
                               exec_history_id: Union[str, int, list, tuple],
                               ret_aggregation_match=False):
        """
        按照record_id查询
        :param exec_history_id:
        :param ret_aggregation_match: 仅返回用于aggregation的match部分
        :return:
        """
        if isinstance(exec_history_id, str):
            to_join = [exec_history_id]
        elif isinstance(exec_history_id, int):
            to_join = [str(exec_history_id)]
        elif isinstance(exec_history_id, (tuple, list)):
            to_join = [str(i) for i in exec_history_id]  # 不确定是不是文本，反正帮它转换了
        else:
            assert 0
        exp = f"^({'|'.join(to_join)})"
        if not ret_aggregation_match:
            t = re.compile(exp)
            return cls.objects.filter(record_id=t)
        else:
            return {
                "$match": {
                    "record_id": {"$regex": exp}
                }
            }


class BaseCapturingDoc(BaseDoc):
    """新的采集基类"""

    _id = ObjectIdField()
    cmdb_id = IntField()
    schema_name = StringField()
    task_record_id = IntField(help_text="在T_TASK_EXEC_HISTORY的id")
    etl_date = DateTimeField(default=lambda: arrow.now().datetime)

    meta = {
        'abstract': True,
        "indexes": [
            "cmdb_id",
            "schema_name",
            "etl_date",
        ]
    }

    @classmethod
    def command_to_execute(cls, obj_owner) -> str:
        """返回本类采集所需的语句（SQL）"""
        raise NotImplementedError

    @classmethod
    def post_captured(cls, docs: list, obj_owner: str, cmdb_id: int, task_record_id):
        """采集到原始数据后，在文档保存之前，做一些操作。一般不需要改"""
        etl_date = arrow.now().datetime
        for d in docs:
            d.from_dict({
                "cmdb_id": cmdb_id,
                "schema_name": obj_owner,
                "task_record_id": task_record_id,
                "etl_date": etl_date
            })
