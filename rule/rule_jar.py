# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseRuleJar",
    "RuleJar",
    "RuleCartridgeJar"
]

from typing import List, Optional

from rule.cmdb_rule import CMDBRule
from rule.rule_cartridge import RuleCartridge


class BaseRuleJar(list):
    """
    规则弹仓
    因为无论线上还是线下，分析的时间可能比较长。在分析的过程禁止用户编辑规则是不可能的。
    所以暂存规则在某个时间点的快照是有必要的。
    TODO 任何时候加入新的规则，都只取启用的规则。
    """

    RULE_MODEL = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.entries: [str] = []

    @classmethod
    def gen_jar_with_entries(cls, *args: [str], **kwargs) -> "BaseRuleJar":
        """
        以entries产生jar
        :param args: entries...
        :return:
        """
        new_jar = cls(cls.RULE_MODEL.filter_enabled(entries__all=args).filter(**kwargs))
        new_jar.entries = args
        return new_jar

    def get_unique_keys(self) -> List[tuple]:
        """获取规则的唯一键值去重列表"""
        return list({i.unique_key() for i in self})

    def bulk_to_dict(self, *args, **kwargs):
        return [i.to_dict(*args, **kwargs) for i in self]


class RuleJar(BaseRuleJar):
    """适用于纳管库规则的规则弹仓"""

    RULE_MODEL = CMDBRule

    def get_rule(self, cmdb_id: int, name: str) -> Optional[CMDBRule]:
        """在jar中搜寻特定规则，如未找到则从数据库中加载"""
        for i in self:
            if i.unique_key() == (cmdb_id, name):
                return i
        the_rule = CMDBRule.filter_enabled(
            cmdb_id=cmdb_id, name=name).first()
        if the_rule:
            self.append(the_rule)
            return the_rule


class RuleCartridgeJar(BaseRuleJar):
    """适用于规则墨盒的规则弹仓"""

    RULE_MODEL = RuleCartridge

