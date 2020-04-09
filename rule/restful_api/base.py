# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Callable

from schema import Use

import cmdb.const
import rule.const
from auth.restful_api.base import *
from utils.schema_utils import *
from ..rule import BaseRule, RuleInputParams, RuleOutputParams


class BaseRuleHandler(AuthReq):

    def _rule_params(self) -> dict:
        return {
            "name": scm_unempty_str,
            "desc": scm_str,
            "unit": scm_str,
            "data_type": self.scm_one_of_choices(
                rule.const.ALL_RULE_PARAM_TYPES),
        }

    def _rule_input_params(self) -> dict:
        """输入参数"""
        ret = self._rule_params()
        ret.update({
            "value": object
        })
        return ret

    def _rule_output_params(self) -> dict:
        """输出参数"""
        ret = self._rule_params()
        ret.update({
            "optional": scm_bool
        })
        return ret

    def scm_rule_input_params(self) -> Use:
        """验证输入参数"""
        return Use(
            lambda x: RuleInputParams(
                **Schema(self._rule_input_params()).validate(x)
            )
        )

    def scm_rule_output_params(self) -> Use:
        """验证输出参数"""
        return Use(
            lambda x: RuleOutputParams(
                **Schema(self._rule_output_params()).validate(x)
            )
        )

    @staticmethod
    def scm_rule_params_list(params) -> Callable:
        """参数列表"""
        return

    def base_rule_schema_for_whole_updating(self) -> dict:
        """整体更新"""
        return {
            # 用于更新的schema。不包含查询字段

            scm_optional("desc"): scm_unempty_str,
            scm_optional("db_type"): self.scm_one_of_choices(
                cmdb.const.ALL_DB_TYPE),
            scm_optional("entries"): self.scm_subset_of_choices(
                rule.const.ALL_RULE_ENTRIES),
            scm_optional("input_params"): [self.scm_rule_input_params()],
            scm_optional("output_params"): [self.scm_rule_output_params()],
            scm_optional("code"): scm_unempty_str,
            scm_optional("status"): scm_bool,
            scm_optional("summary"): scm_str,
            scm_optional("solution"): scm_deduplicated_list,
            scm_optional("weight"): scm_float,
            scm_optional("max_score"): scm_gt0_int,
            scm_optional("level"): self.scm_one_of_choices(
                rule.const.ALL_RULE_LEVELS)
        }

    def base_rule_schema_for_adding(self) -> dict:
        """新增"""
        keys_available_for_updating = {
            k.schema if isinstance(k, scm_optional) else k: v
            for k, v in self.base_rule_schema_for_whole_updating().items()
        }
        return {
            "name": scm_unempty_str,
            **keys_available_for_updating
        }

    def base_rule_schema_for_partly_updating(self) -> dict:
        """更新部分参数"""
        return {
            # 用于更新的schema。不包含查询字段

            scm_optional("entries"): {
                scm_optional("add"): self.scm_one_of_choices(
                    rule.const.ALL_RULE_ENTRIES),
                scm_optional("delete"): scm_str,
            },
            scm_optional("input_params"): {
                scm_optional("add"): self.scm_rule_input_params(),
                scm_optional("update"): self.scm_rule_input_params(),
                scm_optional("delete"): self.scm_rule_input_params(),
            },
            scm_optional("output_params"): {
                scm_optional("add"): self.scm_rule_output_params(),
                scm_optional("update"): self.scm_rule_output_params(),
                scm_optional("delete"): self.scm_rule_output_params(),
            }
        }
