# Author: kk.Fang(fkfkbill@gmail.com)

from mongoengine import StringField, IntField, DateTimeField, FloatField

from .offline import TicketSQLPlan


class MySQLTicketSQLPlan(TicketSQLPlan):
    """mysql工单动态审核产生的执行计划"""

    meta = {
        "collection": "mysql_ticket_sql_plan",
        'indexes': []
    }
