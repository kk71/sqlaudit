# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models
init_models()

from rule.cmdb_rule_utils import initiate_cmdb_rule


@click.argument("cmdb_id", type=click.INT, required=True)
def main(cmdb_id):
    """initiate cmdb-rules in cmdb, force clear all old rules"""
    print(f"going to initiate cmdb-rule in cmdb({cmdb_id}) ...")
    print(f"done with cmdb-rules num: {initiate_cmdb_rule(cmdb_id)}")
