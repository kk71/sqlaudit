# Author: kk.Fang(fkfkbill@gmail.com)

import uuid

from mongoengine import IntField, StringField, DateTimeField, FloatField, \
    EmbeddedDocument, EmbeddedDocumentListField

from core.ticket import *
from new_models.mongoengine import *
from utils import datetime_utils
from . import const


class TicketScript(EmbeddedDocument, BaseTicketScript, metaclass=ABCDocumentMetaclass):
    """工单脚本"""

    script_id = StringField(required=True)
    script_name = StringField()
    dir_path = StringField()
    sub_ticket_count = IntField(default=0)

    def generate_new_script_id(self):
        """
        生成新的唯一脚本id
        :return: new script id
        """
        new_script_id = uuid.uuid4().hex
        self.script_id = new_script_id
        return new_script_id


class Ticket(BaseDoc, BaseTicket, metaclass=ABCTopLevelDocumentMetaclass):
    """工单"""

    task_name = StringField(required=True)
    db_type = StringField()
    cmdb_id = IntField()
    sub_ticket_count = IntField(default=0)
    scripts = EmbeddedDocumentListField(TicketScript)
    submit_time = DateTimeField(default=lambda: datetime_utils.datetime.now())
    submit_owner = StringField()
    status = IntField(choices=const.ALL_TICKET_STATUS)
    audit_role_id = IntField()
    audit_owner = StringField()
    audit_time = DateTimeField()
    audit_comment = StringField(default="")
    online_time = DateTimeField()
    score = FloatField()

    meta = {
        "allow_inheritance": True,
        "collection": "ticket",
        'indexes': [
            "task_name",
            "db_type",
            "cmdb_id",
            "submit_time",
            "submit_owner",
            "status",
            "audit_role_id"
        ]
    }

    def calculate_score(self, *args, **kwargs):
        """
        计算工单当前分数
        :param args:
        :param kwargs:
        :return:
        """
        return
