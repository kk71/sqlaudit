# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketAuditProcessTemplate",
    "TicketManualAudit"
]

from mongoengine import StringField, EmbeddedDocumentListField,\
    DynamicEmbeddedDocument, IntField

from models.mongoengine import BaseDoc


class TicketManualAudit(DynamicEmbeddedDocument):
    """工单人工审核信息"""

    audit_role_id = IntField()
    audit_role_name = StringField()

    meta = {
        "allow_inheritance": True
    }


class TicketAuditProcessTemplate(BaseDoc):
    """工单审核流程模板"""

    name = StringField(required=True)
    process = EmbeddedDocumentListField(TicketManualAudit)

    meta = {
        "collection": "ticket_audit_process_template",
        "indexes": [
            {'fields': ("name",), 'unique': True},
            {'fields': ("process__audit_role_id",), 'unique': True},
        ]
    }
