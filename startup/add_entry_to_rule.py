# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from new_rule.rule import TicketRule
from new_rule import const


def main():
    """FOR DEVELOPMENT: add new entry to rule"""
    TicketRule.objects(
        entries__in=[const.RULE_ENTRY_TICKET_STATIC]).update(
        entries__insert=const.RULE_ENTRY_TICKET_STATIC_CMDB_INDEPENDENT)

