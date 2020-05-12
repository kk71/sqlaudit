# Author: kk.Fang(fkfkbill@gmail.com)

import rule.const
from auth.restful_api.base import *
from restful_api.modules import *


@as_view("param_type", group="rule")
class ParamTypeHandler(AuthReq):

    def get(self):
        """规则输入输出参数的data_type,不分页"""
        self.resp(rule.const.ALL_RULE_PARAM_TYPES)


@as_view("entries", group="rule")
class EntryHandler(AuthReq):

    def get(self):
        """规则的可用entries列表,不分页"""
        self.resp(rule.const.ALL_RULE_ENTRIES)


@as_view("level_score", group="rule")
class LevelScoreHandler(AuthReq):

    def get(self):
        """规则等级和最高分，权重的关系，不分页"""
        self.resp(rule.const.RULE_LEVEL_SCORE)
