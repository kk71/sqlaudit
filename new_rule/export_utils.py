# Author: kk.Fang(fkfkbill@gmail.com)

import json

from new_rule.rule import TicketRule


def ticket_rule_import(filename) -> tuple:
    """导入工单规则，去重"""
    with open(filename, "r") as z:
        rules = json.load(z)
    rules_to_import = []
    for rule in rules:
        the_rule = TicketRule()
        the_rule.from_dict(rule, iter_if=lambda k, v: k not in ("_id", "id"))
        if TicketRule.objects(**the_rule.to_dict(iter_if=lambda k, v: k in (
                "db_type", "name"))).count():
            print(f"this ticket rule existed: {the_rule.unique_key()}")
            continue
        rules_to_import.append(the_rule)
    if rules_to_import:
        TicketRule.objects.insert(rules_to_import)
    return len(rules_to_import), len(rules)


def ticket_rule_export(filename) -> int:
    """导出工单规则，覆盖给定的文件"""
    rules = [i.to_dict(iter_if=lambda k, v: k not in ("_id", "id"))
             for i in TicketRule.objects()]
    with open(filename, "w") as z:
        z.write(json.dumps(rules, indent=4, ensure_ascii=False))
    return len(rules)


def ticket_rule_drop() -> int:
    """删除库中全部的工单规则"""
    deleted_num = TicketRule.objects().delete()
    return deleted_num
