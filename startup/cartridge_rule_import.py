# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from rule.rule_cartridge import RuleCartridge
from rule.rule_cartridge_utils import rule_import


@click.option(
    "--filename",
    help="the json filename",
    default=RuleCartridge.DEFAULT_JSON_FILE,
    type=click.STRING)
def main(filename):
    """import rule cartridge, deduplicated."""
    print(f"going to import ticket rules from {filename} ...")
    imported_num = rule_import(filename)
    print(f"{imported_num} rule(s) imported.")



