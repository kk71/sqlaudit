# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField

import ticket.sub_ticket


class OracleSubTicket(ticket.sub_ticket.SubTicket):
    """oracle的子工单"""
    schema_name = StringField(required=True)

    meta = {
        'indexes': [
            "schema_name",
        ]
    }
