# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField

import ticket.ticket


class OracleTicket(ticket.ticket.Ticket):
    """oracle工单"""

    online_username = StringField(default="")
    online_password = StringField(default="")
