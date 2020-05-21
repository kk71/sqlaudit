# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "rule_drop",
    "rule_export",
    "rule_import",
    "update_code"
]

import json
import traceback

import cmdb.const
from rule.rule_cartridge import RuleCartridge


def rule_import(filename) -> tuple:
    """导入规则，去重"""
    with open(filename, "r") as z:
        rules = json.load(z)
    rules_to_import = []
    for rule in rules:
        if "db_model" not in rule.keys():
            rule["db_model"] = cmdb.const.MODEL_OLTP
            print(f"this one has no db_model, "
                  f"and give it {cmdb.const.MODEL_OLTP} as default: {rule}")
        the_rule = RuleCartridge()
        the_rule.from_dict(rule, iter_if=lambda k, v: k not in (
            "_id", "id", "create_time"))
        if RuleCartridge.filter(**the_rule.unique_key(as_dict=True)).count():
            print(f"this ticket rule existed: {the_rule.unique_key()}")
            continue
        rules_to_import.append(the_rule)
    if rules_to_import:
        RuleCartridge.objects.insert(rules_to_import)
    return len(rules_to_import), len(rules)


def rule_export(filename) -> int:
    """导出规则，覆盖给定的文件"""
    rules = [i.to_dict(iter_if=lambda k, v: k not in ("_id", "id", "create_time"))
             for i in RuleCartridge.filter()]
    with open(filename, "w") as z:
        z.write(json.dumps(rules, indent=4, ensure_ascii=False))
    return len(rules)


def rule_drop() -> int:
    """删除库中全部的规则"""
    deleted_num = RuleCartridge.filter().delete()
    return deleted_num


def update_code(compare: bool):
    """
    更新代码仓库的规则代码到对应的规则墨盒里
    :param compare: True == 只检查本地与规则墨盒的代码的差异，并不写入更新
    :return:
    """
    if compare:
        print("=== compare only ===")
    different_codes = []
    not_imported_rules = []
    for tr in RuleCartridge.filter().all():
        try:
            code_file = RuleCartridge.CODE_FILES_DIR / f"{tr.db_type}/{tr.name}.py"
            if not code_file.exists():
                raise Exception(f"code file {code_file} not existed.")
            if not code_file.is_file():
                raise Exception(f"{code_file} is not a file.")
            with open(str(code_file), "r") as z:
                new_code = z.read()
                if tr.code != new_code:
                    different_codes.append(tr.unique_key())
                    if not compare:
                        tr.code = new_code
                        tr.test()
                        tr.save()
        except Exception as e:
            print(traceback.format_exc())
            not_imported_rules.append(str(tr))
    if not compare:
        print(f"{len(different_codes)} rules updated in code: {different_codes}")
        print(f"{len(not_imported_rules)} rules not updated "
              f"with exceptions above: {not_imported_rules}")
    else:
        print(f"{len(different_codes)} rules different in code "
              f"and local code files: {different_codes}")

