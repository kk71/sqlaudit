# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

# initiate database models/connections

init_models()

from rule.rule_cartridge_utils import rule_drop


def main():
    """clear rule cartridge, use with caution!"""
    dropped_num = rule_drop()
    print(f"{dropped_num} ticket rule(s) dropped.")



