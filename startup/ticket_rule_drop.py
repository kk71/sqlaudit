# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

# initiate database models/connections

init_models()

from rule.export_utils import rule_drop


def main():
    """delete all ticket rules, use with caution!"""
    dropped_num = rule_drop()
    print(f"{dropped_num} ticket rule(s) dropped.")



