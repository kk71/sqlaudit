# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField

import ticket.ticket


class OracleTicket(ticket.ticket.Ticket):
    """oracle工单"""

    schema_name = StringField()
    online_username = StringField(default="")
    online_password = StringField(default="")
