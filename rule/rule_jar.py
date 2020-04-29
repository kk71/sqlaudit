# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "RuleJar"
]

from typing import List


class RuleJar(list):
    """
    存放规则的内存对象。
    因为无论线上还是线下，分析的时间可能比较长。在分析的过程禁止用户编辑规则是不可能的。
    所以暂存规则在某个时间点的快照是有必要的。
    """
    def __init__(self, *args, **kwargs):
        super(RuleJar, self).__init__(*args, **kwargs)
        self.entries: [str] = []

    @classmethod
    def gen_jar_with_entries(cls, *args: [str], **kwargs) -> "RuleJar":
        """
        以entries产生jar
        TODO 只取启用的规则。
        :param args: entries...
        :return:
        """
        from rule.cmdb_rule import CMDBRule
        new_jar = cls(CMDBRule.filter_enabled(entries__all=args).filter(**kwargs))
        new_jar.entries = args
        return new_jar

    def get_unique_keys(self) -> List[tuple]:
        """获取规则的唯一键值去重列表"""
        return list({i.unique_key() for i in self})

    def bulk_to_dict(self, *args, **kwargs):
        return [i.to_dict(*args, **kwargs) for i in self]
