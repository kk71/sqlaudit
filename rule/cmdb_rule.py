# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "CMDBRule"
]

from typing import Optional, List

from mongoengine import IntField

from rule import const
from rule.rule_cartridge import RuleCartridge
from rule.rule import BaseRule


class CMDBRule(BaseRule):
    """纳管库规则"""

    cmdb_id = IntField(required=True, null=False)

    UNIQUE_KEYS = ("cmdb_id", "name")

    meta = {
        "collection": "cmdb_rule",
        "indexes": [
            {'fields': UNIQUE_KEYS, 'unique': True},
            *UNIQUE_KEYS
        ]
    }

    def from_rule_cartridge(
            self,
            rule_cartridge: RuleCartridge,
            keys_excluded=const.KEYS_NOT_SYNCHRONIZED_FROM_RULE_CARTRIDGE,
            keys_warns=const.WARN_KEYS_TO_SYNCHRONIZE_WHEN_DIFFERENT,
            force: bool = False) -> Optional[List[str]]:
        """
        从规则墨盒更新到纳管库规则
        :param rule_cartridge:
        :param keys_excluded: 不要更新的纳管库规则字段
        :param keys_warns: 当这些纳管库规则字段与墨盒规则库字段不一致的时候，询问是否更新
        :param force: 强制更新规则，不保留原来纳管库规则的任何数据，不产生任何询问
        :return: 除非force==True,不然返回两者不一样的字段名列表(in keys_warns)
        """
        different_keys = []
        for k in super().ALL_KEYS:
            cartridge_value = getattr(rule_cartridge, k)
            cmdb_value = getattr(self, k)
            if force:
                setattr(self, k, cartridge_value)
                continue
            if k in keys_excluded:
                continue
            if k in keys_warns and cmdb_value != cartridge_value:
                different_keys.append(k)
        if not force:
            return different_keys
