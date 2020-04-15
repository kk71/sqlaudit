# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseRuleHandler"
]

from schema import Use, Or

import cmdb.const
import rule.const
from .. import exceptions
from auth.restful_api.base import *
from utils.schema_utils import *
from ..rule import RuleInputParams, RuleOutputParams
from .. import const
from ..rule import BaseRule


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
        data_type = ret["data_type"]
        if data_type == const.RULE_PARAM_TYPE_STR:
            data_type = scm_str
        elif data_type == const.RULE_PARAM_TYPE_INT:
            data_type = scm_int
        elif data_type == const.RULE_PARAM_TYPE_FLOAT:
            data_type = scm_float
        elif data_type == const.RULE_PARAM_TYPE_NUM:
            data_type = scm_num
        elif data_type == const.RULE_PARAM_TYPE_LIST:
            data_type = Or(list, scm_dot_split_str, scm_dot_split_int)
        else:
            assert 0
        ret.update({
            "value": data_type
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
        def _scm(x):
            rip = RuleInputParams(
                **Schema(self._rule_input_params()).validate(x)
            )
            try:
                rip.validate_input_data()
            except exceptions.RuleCodeInvalidParamTypeException:
                return self.resp_bad_req(f"输入参数值与类型不匹配: {x}")
            return rip

        return Use(_scm)

    def scm_rule_output_params(self) -> Use:
        """验证输出参数"""
        return Use(
            lambda x: RuleOutputParams(
                **Schema(self._rule_output_params()).validate(x)
            )
        )

    @staticmethod
    def scm_list_of_dict_duplication(x):
        if not len({i["name"] for i in x}) == len(x):
            scm_raise_error("参数name重复")
        return True

    def base_rule_schema_for_whole_updating(self) -> dict:
        """整体更新"""
        return {
            # 用于更新的schema。不包含查询字段

            scm_optional("desc"): scm_unempty_str,
            scm_optional("db_type"): self.scm_one_of_choices(
                cmdb.const.ALL_DB_TYPE),
            scm_optional("entries"): self.scm_subset_of_choices(
                rule.const.ALL_RULE_ENTRIES),
            scm_optional("input_params"): Or(
                [],
                And(self.scm_list_of_dict_duplication, [self.scm_rule_input_params()]),
            ),
            scm_optional("output_params"): Or(
                [],
                And(self.scm_list_of_dict_duplication, [self.scm_rule_output_params()]),
            ),
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

    def base_rule_schema_for_special_updating(self) -> dict:
        """特殊更新参数"""
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

    def special_update(
            self,
            rule_item: BaseRule,
            entries=None,
            input_params=None,
            output_params=None):
        """更新规则的特殊字段"""
        if entries:
            if entries.get("add", None):
                new_entry_to_add: str = entries["add"]
                if new_entry_to_add in rule_item.entries:
                    return self.resp_bad_req(msg=f"{new_entry_to_add}输入点已存在")
                rule_item.entries.append(new_entry_to_add)
            if entries.get("delete", None):
                entry_to_delete: str = entries["delete"]
                if entry_to_delete not in rule_item.entries:
                    return self.resp_bad_req(msg=f"{entry_to_delete}不存在")
                rule_item.entries = [i
                                     for i in rule_item.entries
                                     if i != entry_to_delete]
        if input_params:
            if input_params.get("add", None):
                current_input_params_name: set = {
                    i.name
                    for i in rule_item.input_params
                }
                new_input_params_to_add: RuleInputParams = input_params["add"]
                if new_input_params_to_add.name in current_input_params_name:
                    return self.resp_bad_req(msg=f"{new_input_params_to_add.name}已存在")
                rule_item.input_params.append(new_input_params_to_add)
            if input_params.get("update", None):
                input_params_to_update: RuleInputParams = input_params["update"]
                rule_item.input_params = [
                    an_input_params
                    for an_input_params in rule_item.input_params
                    if an_input_params.name != input_params_to_update.name
                ]
                rule_item.input_params.append(input_params_to_update)
            if input_params.get("delete", None):
                input_params_to_delete: RuleInputParams = input_params["delete"]
                rule_item.input_params = [
                    an_input_params
                    for an_input_params in rule_item.input_params
                    if an_input_params.name != input_params_to_delete.name
                ]
        if output_params:
            if output_params.get("add", None):
                current_output_params_name: set = {
                    i.name
                    for i in rule_item.output_params
                }
                new_output_params_to_add: RuleOutputParams = output_params["add"]
                if new_output_params_to_add.name in current_output_params_name:
                    return self.resp_bad_req(msg=f"{new_output_params_to_add.name}已存在")
                rule_item.output_params.append(new_output_params_to_add)
            if output_params.get("update", None):
                output_params_to_update: RuleOutputParams = output_params["update"]
                rule_item.output_params = [
                    an_output_params
                    for an_output_params in rule_item.output_params
                    if an_output_params.name != output_params_to_update.name
                ]
                rule_item.output_params.append(output_params_to_update)
            if output_params.get("delete", None):
                output_params_to_delete: RuleOutputParams = output_params["delete"]
                rule_item.output_params = [
                    an_output_params
                    for an_output_params in rule_item.output_params
                    if an_output_params.name != output_params_to_delete.name
                ]

    def test_code(self, a_rule_item):
        try:
            a_rule_item.test()
        except exceptions.RuleCodeInvalidException:
            return self.resp_bad_req(msg="规则代码错误")
