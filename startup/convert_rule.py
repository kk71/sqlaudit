# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from new_rule.rule import TicketRule
from new_rule import const
import utils.const


def main():
    """DO NOT USE IT!!!"""
    for r in TicketRule.objects():
        r.entries = [const.RULE_ENTRY_TICKET]
        if r.analyse_type == "STATIC":
            r.entries += [const.RULE_ENTRY_TICKET_STATIC]
        if r.analyse_type == "DYNAMIC":
            r.entries += [const.RULE_ENTRY_TICKET_DYNAMIC]
        if r.sql_type == utils.const.SQL_DML:
            r.entries += [const.RULE_ENTRY_DML]
        if r.sql_type == utils.const.SQL_DDL:
            r.entries += [const.RULE_ENTRY_DDL]
        del r.analyse_type
        del r.sql_type
        r.save()
