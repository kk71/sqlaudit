# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from utils import rule_utils


@click.option(
    "--filename", help="the json filename", default="./files/rule.json", type=click.STRING)
def main(filename):
    """export rules to a json file"""
    print(f"going to export rule to {filename}...")
    all_num = rule_utils.export_rule_to_json_file(filename)
    print(f"Done({all_num}).")
