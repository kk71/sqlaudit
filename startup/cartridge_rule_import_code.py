# Author: kk.Fang(fkfkbill@gmail.com)

import traceback

import click

from models import init_models

# initiate database models/connections

init_models()

from rule.rule_cartridge_utils import *


@click.option(
    "--compare",
    help="don't import, compare only.",
    default=False,
    type=click.BOOL)
def main(compare: bool):
    """FOR DEVELOPMENT: import rule code to rule cartridge"""
    update_code(compare)
