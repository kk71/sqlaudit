# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from rule.rule_cartridge_utils import rule_import


@click.option(
    "--filename",
    help="the json filename",
    default="./new_rule/files/ticket-rule.json",
    type=click.STRING)
def main(filename):
    """import ticket rules, deduplicated."""
    print(f"going to import ticket rules from {filename} ...")
    imported_num = rule_import(filename)
    print(f"{imported_num} rule(s) imported.")



