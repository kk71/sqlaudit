# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from utils import rule_utils


@click.option(
    "--filename", help="the json filename", default="./files/rule.json", type=click.STRING)
def main(filename):
    """import json file to mongo rules, deduplicated"""
    print(f"going to import rule from {filename}...")
    imported_num, all_num = rule_utils.import_from_json_file(filename)
    print(f"Done({imported_num} of {all_num}).")
    # if input("Do you want to set all rules as risk rule? y/n") == "y":
    # if True:
    #     print("going to import risk rules...")
    #     from models.oracle import make_session
    #     with make_session() as session:
    #         r = rule_utils.set_all_rules_as_risk_rule(session)
    #     print(f"Done({r})")


