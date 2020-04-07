# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

import rule.const
from rule.rule import Rule


def main():
    """FOR DEVELOPMENT: add new entry to rule"""
    # TicketRule.objects(
    #     entries__in=[const.RULE_ENTRY_TICKET_STATIC]).update(
    #     push__entries=const.RULE_ENTRY_TICKET_STATIC_CMDB_INDEPENDENT)
    pass

