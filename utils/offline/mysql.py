# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "MySQLSubTicketAnalysis",
]

import re
import uuid

import sqlparse
from mongoengine import QuerySet as mongoengine_qs
from sqlalchemy.orm.query import Query as sqlalchemy_qs

from models.mongo import *
from models.oracle import CMDB, WorkList
from plain_db.oracleob import *
from utils.const import *
from utils.datetime_utils import *
from .base import SubTicketAnalysis


class MySQLSubTicketAnalysis(SubTicketAnalysis):
    """mysql子工单分析模块"""

    db_type = DB_MYSQL

    def __init__(self,
                 static_rules_qs: mongoengine_qs = None,
                 dynamic_rules_qs: mongoengine_qs = None):
        if static_rules_qs is None:
            static_rules_qs = TicketRule.filter_enabled(
                analyse_type=TICKET_RULE_STATIC,
                db_type=DB_MYSQL
            )
        if dynamic_rules_qs is None:
            dynamic_rules_qs = TicketRule.filter_enabled(
                type=TICKET_RULE_DYNAMIC,
                db_type=DB_MYSQL
            )
        super(MySQLSubTicketAnalysis, self).__init__(static_rules_qs, dynamic_rules_qs)
