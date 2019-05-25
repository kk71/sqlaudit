# Author: kk.Fang(fkfkbill@gmail.com)

from typing import *

from mongoengine import Q

from backend.models.mongo import Results, Rule
from backend.models.oracle import CMDB
from backend.utils.cmdb_utils import CMDBNotFoundException
from backend.utils import rule_utils

# TODO NOT FINISHED YET!
# TODO NOT FINISHED YET!
# TODO NOT FINISHED YET!


def get_result_query_by(
        session,
        rule_type: Union[str, list, tuple],
        cmdb_id: int):
    """
    根据条件查询result
    :param session:
    :param rule_type: 规则类型，单个字符串或者list of str指明多个类型
    :param cmdb_id:
    :return: 返回是一个Results的queryset
    """

    def gen_query_condition(rule_type, rule_name):
        if rule_type == rule_utils.RULE_TYPE_OBJ:
            return Q(**{
                f"{rule_name}__records__exists": 1,
                f"{rule_name}__scores__exists": 1,
            })
        else:
            return Q(**{
                f"{rule_name}__records__exists": 1,
                f"{rule_name}__scores__exists": 1,
            })

    if isinstance(rule_type, str):
        rule_type = (rule_type,)
    elif isinstance(rule_type, (list, tuple)):
        pass
    else:
        assert 0
    cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
    if not cmdb:
        raise CMDBNotFoundException
    rules = Rule.objects(rule_type__in=rule_type, db_model=cmdb.db_model)
    rule_name_list = list(rules.values_list("rule_name", flat=True))
    result_q = Results.objects()
    Qs = None
    # for rule_name in rule_name_list:
    #     if not Qs:
    #         Qs = Q(rule_name__)
