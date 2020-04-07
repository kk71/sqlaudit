# Author: kk.Fang(fkfkbill@gmail.com)

import json

from rule.rule import Rule


def rule_import(filename) -> tuple:
    """导入规则，去重"""
    with open(filename, "r") as z:
        rules = json.load(z)
    rules_to_import = []
    for rule in rules:
        the_rule = Rule()
        the_rule.from_dict(rule, iter_if=lambda k, v: k not in ("_id", "id"))
        if Rule.objects(**the_rule.to_dict(iter_if=lambda k, v: k in (
                "db_type", "name"))).count():
            print(f"this ticket rule existed: {the_rule.unique_key()}")
            continue
        rules_to_import.append(the_rule)
    if rules_to_import:
        Rule.objects.insert(rules_to_import)
    return len(rules_to_import), len(rules)


def rule_export(filename) -> int:
    """导出规则，覆盖给定的文件"""
    rules = [i.to_dict(iter_if=lambda k, v: k not in ("_id", "id", "create_time"))
             for i in Rule.objects()]
    with open(filename, "w") as z:
        z.write(json.dumps(rules, indent=4, ensure_ascii=False))
    return len(rules)


def rule_drop() -> int:
    """删除库中全部的规则"""
    deleted_num = Rule.objects().delete()
    return deleted_num
