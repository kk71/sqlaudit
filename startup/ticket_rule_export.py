# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from new_rule.export_utils import rule_export


@click.option(
    "--filename",
    help="the json filename",
    default="./files/ticket-rule.json",
    type=click.STRING)
def main(filename):
    """export ticket rules, target json file will be overwritten if existed."""
    print(f"going to export ticket rules to {filename} ...")
    exported_num = rule_export(filename)
    print(f"{exported_num} rule(s) exported.")


