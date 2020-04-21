# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "initiate_cmdb_rule",
    "cmdb_rule_update"
]

from typing import Union, List

from models.sqlalchemy import *
from models.mongoengine import *
from cmdb.cmdb import CMDB
from .rule_cartridge import RuleCartridge
from .cmdb_rule import CMDBRule


def initiate_cmdb_rule(cmdb_id: int, db_type: str, db_model: str) -> int:
    """
    给一个纳管库分配纳管库规则
    :param cmdb_id:
    :param db_type:
    :param db_model:
    :return: 插入的纳管库规则数
    """
    rules = RuleCartridge.objects(
        cmdb_id=cmdb_id, db_type=db_type, db_model=db_model)
    rules_to_insert_into_cmdb_rule = []
    for a_rule_cartridge in rules:
        a_cmdb_rule = CMDBRule(cmdb_id=cmdb_id)
        a_cmdb_rule.from_rule_cartridge(a_rule_cartridge, force=True)
        rules_to_insert_into_cmdb_rule.append(a_cmdb_rule)
    return len(CMDBRule.objects.insert(rules_to_insert_into_cmdb_rule))


def cmdb_rule_update(
        cmdb_id: int,
        db_type: str,
        db_model: str,
        rule_name: Union[List[str], str] = None,
        **kwargs):
    """
    纳管库规则从规则墨盒更新
    :param cmdb_id:
    :param db_type:
    :param db_model:
    :param rule_name: 不指明规则名称，则使用纳管库的信息更新全部规则
    :param kwargs: 传递给CMDBRule.from_cartridge_rule
    :return:
    """
    rules: mongoengine_qs = RuleCartridge.objects(
        cmdb_id=cmdb_id, db_type=db_type, db_model=db_model)
    if rule_name:
        if isinstance(rule_name, str):
            rule_name = [rule_name]
        elif isinstance(rule_name, (tuple, list)):
            pass
        else:
            assert 0
        rules: mongoengine_qs = rules.filter(name__in=rule_name)
    # TODO
    return
