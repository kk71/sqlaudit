# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "initiate_cmdb_rule",
    "cmdb_rule_update"
]

from models.sqlalchemy import *
from cmdb.cmdb import CMDB
from .rule import CMDBRule, RuleCartridge
from . import const


def initiate_cmdb_rule(cmdb_id: int, db_type: str, db_model: str):
    """
    给一个纳管库分配纳管库规则
    :param cmdb_id:
    :param db_type:
    :param db_model:
    :return:
    """
    return


def cmdb_rule_update(
        cmdb_id: int,
        rule_name: [str] = None,
        keys_excluded=const.KEYS_NOT_SYNCHRONIZED_FROM_RULE_CARTRIDGE,
        keys_warns=const.WARN_KEYS_TO_SYNCHRONIZE_WHEN_DIFFERENT,
        force: bool = False):
    """
    纳管库规则从规则墨盒更新
    :param cmdb_id:
    :param rule_name: 不指明规则名称，则使用纳管库的信息更新全部规则
    :param keys_excluded: 不要更新的纳管库规则字段
    :param keys_warns: 当这些纳管库规则字段与墨盒规则库字段不一致的时候，询问是否更新
    :param force: 强制更新规则，不保留原来纳管库规则的任何数据，不产生任何询问
    :return:
    """
    return
