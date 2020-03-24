# Author: kk.Fang(fkfkbill@gmail.com)

import abc

from mongoengine import StringField, IntField

from new_models.mongoengine import *


class TicketSQLPlan(BaseDoc, abc.ABCMeta, metaclass=ABCTopLevelDocumentMetaclass):
    """工单动态审核产生的执行计划，基类"""

    statement_id = StringField(primary_key=True)
    ticket_id = IntField(required=True)
    cmdb_id = IntField(required=True)

    meta = {
        'abstract': True,
        'indexes': [
            "statement_id",
            "ticket_id",
            "cmdb_id",
        ]
    }

    @classmethod
    @abc.abstractmethod
    def add_from_dict(cls,
                      statement_id: str,
                      ticket_id: str,
                      cmdb_id: int, **kwargs) -> list:
        """从字典增加执行计划"""
        pass

