# Author: kk.Fang(fkfkbill@gmail.com)

import re
import json

import chardet
from mongoengine import Q

from models.oracle import RiskSQLRule, WhiteListRules, CMDB
from models.mongo import *
from utils.perf_utils import *
from utils.const import *


def format_rule_result_detail(rule_object, record: list):
    """
    格式化输出规则分析结果(即风险详情)的信息。信息来源与mongodb.results.(rule_name).records
    :return:
    """
    output_params = rule_object.output_parms
    risk_detail = ', '.join([': '.join([
        output_params[index]['parm_desc'], str(value)]) for index, value in enumerate(record)])
    return risk_detail


def export_rule_to_json_file(filename: str):
    """导出rule"""
    rules = [i.to_dict(iter_if=lambda k, v: k not in ("_id",)) for i in Rule.objects()]
    with open(filename, "w") as z:
        z.write(json.dumps(rules, indent=4, ensure_ascii=False))
    return len(rules)


def import_from_json_file(filename: str):
    """
    从json文件导入规则至mongodb
    :param filename:
    :return: 导入数, 总共数
    """
    with open(filename, "r") as z:
        rules = json.load(z)
    rules_to_import = []
    for rule in rules:
        the_rule = Rule()
        the_rule.from_dict(rule, iter_if=lambda k, v: k not in ("_id", ))
        if Rule.objects(**the_rule.to_dict(iter_if=lambda k, v: k in (
                "db_type", "db_model", "rule_name"))).count():
            print(f"this rule existed: {the_rule.get_3_key()}")
            continue
        rules_to_import.append(the_rule)
    if rules_to_import:
        Rule.objects.insert(rules_to_import)
    return len(rules_to_import), len(rules)


def set_all_rules_as_risk_rule(session,filename):
    """把当前mongo的全部rule都设置为风险规则"""
    risks = []
    # 需要把DDL的规则排除
    for rule in Rule.objects(sql_type__ne=SQL_DDL):
        key = json.loads(json.dumps(rule.to_dict(iter_if=lambda k, v: k in (
                "rule_name", "rule_type", "db_model", "db_type"))))
        if session.query(RiskSQLRule).filter_by(**key).count():
            continue
        if not isinstance(rule.rule_desc, str):
            print(chardet.detect(rule.rule_desc))
        rr = RiskSQLRule(**key)
        rr.risk_name = json.loads(json.dumps(rule.rule_desc))
        with open(filename,"r") as f:
            risk_rules = json.load(f)
        for r_r in risk_rules:
            if r_r["rule_name"]==rule.rule_name:
                rr.severity=r_r['severity']
        # rr.severity = json.loads(json.dumps("严重"))
        rr.optimized_advice = ", ".join(json.loads(json.dumps(rule.solution)))
        rr.influence =json.loads(json.dumps(rule.rule_summary))
        risks.append(rr)
    session.add_all(risks)
    return len(risks)


def merge_risk_rule_and_rule(
        risk_rule_object, rule_object=None, rule_keys=("rule_desc", "rule_name","weight","max_score")) -> dict:
    """
    merge rule_object to risk_rule object
    :param risk_rule_object:
    :param rule_object:
    :param rule_keys:
    """
    risk_rule_dict = risk_rule_object.to_dict()

    #修改风险规则等级对象的规则的权重和最大扣分
    rule_name = risk_rule_dict['rule_name']
    severity = risk_rule_dict['severity']
    rules_q=Rule.objects(rule_name=rule_name)
    for rule_q in rules_q:
        if severity == RULE_LEVEL_SEVERE:
            rule_q.weight = RULE_LEVEL_SEVERE_WEIGHT
            rule_q.max_score = RULE_LEVEL_SEVERE_MAX_SCORE
        if severity == RULE_LEVEL_WARNING:
            rule_q.weight = RULE_LEVEL_WARNING_WEIGHT
            rule_q.max_score = RULE_LEVEL_WARNING_MAX_SCORE
        if severity == RULE_LEVEL_INFO:
            rule_q.weight = RULE_LEVEL_INFO_WEIGHT
            rule_q.max_score = RULE_LEVEL_INFO_MAX_SCORE
        rule_q.save()

    if not rule_object:
        rule_object = Rule.objects(**risk_rule_object.
                                   to_dict(iter_if=lambda k, v: k in RULE_ALLOCATING_KEYS)).first()
    if not rule_object:  # 用于排错
        print(risk_rule_object.to_dict(iter_if=lambda k, v: k in RULE_ALLOCATING_KEYS))
    rule_dict = rule_object.to_dict(iter_if=lambda k, v: k in rule_keys)
    return {**risk_rule_dict, **rule_dict}


def calc_sum_of_rule_max_score(db_type, db_model, rule_type) -> float:
    """
    计算某个类型的规则的最大分总合
    """
    # TODO make it cached
    rule_q = Rule.filter_enabled(db_model=db_model, rule_type=rule_type)
    return sum([float(rule.max_score) for rule in rule_q])


def get_risk_rules_dict(session) -> dict:
    """
    parse all risk rules into a dict with 3-key indexing
    """
    # TODO make it cached
    risk_rule_list = list(session.query(RiskSQLRule).filter_by().all())
    return {(r.db_type, r.db_model, r.rule_name): r for r in risk_rule_list}


@timing(cache=r_cache)
def get_all_risk_towards_a_sql(session, sql_id, date_range: tuple) -> set:
    """
    用当前配置的风险规则，去遍历results
    :param session:
    :param sql_id:
    :param date_range: a tuple of two datetime objects
    :return: risk rule_name set
    """
    risk_rule_dict = get_risk_rules_dict(session)
    all_risk_rule_name_list = [i[2]  # 0db_type 1 db_model 2rule_name
                                 for i in risk_rule_dict.keys()]
    result_q = Results.objects()
    date_start, date_end = date_range
    if date_start:
        result_q = result_q.filter(create_date__gte=date_start)
    if date_end:
        result_q = result_q.filter(create_date__lte=date_end)
    Qs = None
    for rn in all_risk_rule_name_list:
        if not Qs:
            Qs = Q(**{f"{rn}__sqls__sql_id": sql_id})
        else:
            Qs = Qs | Q(**{f"{rn}__sqls__sql_id": sql_id})
    result_q = result_q.filter(Qs)
    rule_name_set = set()
    for r in result_q:
        for rn in all_risk_rule_name_list:
            if getattr(r, rn, None) and getattr(r, rn).get("sqls"):
                for sql in getattr(r, rn)["sqls"]:
                    if sql["sql_id"] == sql_id:
                        rule_name_set.add(rn)
    return rule_name_set


@timing(cache=r_cache)
def get_risk_rate(session, cmdb_id, date_range: tuple) -> dict:
    """
    获取最近的风险率
    :param session:
    :param cmdb_id:
    :param date_range:
    :return:
    """
    date_start, date_end = date_range
    results_q = Results.objects(cmdb_id=cmdb_id, create_date__gte=date_start,
                                create_date__lte=date_end)
    # OBJ
    tab_info_q = ObjTabInfo.objects(cmdb_id=cmdb_id)
    part_tab_info_q = ObjPartTabParent.objects(cmdb_id=cmdb_id)
    index_q = ObjIndColInfo.objects(cmdb_id=cmdb_id)
    seq_q = ObjSeqInfo.objects(cmdb_id=cmdb_id)
    # others
    sql_text_q = SQLText.objects(cmdb_id=cmdb_id)
    sql_plan_q = MSQLPlan.objects(cmdb_id=cmdb_id)
    sql_stats_q = SQLStat.objects(cmdb_id=cmdb_id)
    # rule dict for searching obj_info_type
    db_model = session.query(CMDB.db_model).filter(CMDB.cmdb_id == cmdb_id)[0][0]
    rule_q = Rule.filter_enabled(db_model=db_model)
    ret = {
        # OBJ
        "table": {"violation_num": 0, "sum": 0, "rate": 0.0},  # table包括普通表及分区表,无属性或类型可定义，故以table表示
        OBJ_RULE_TYPE_SEQ: {"violation_num": 0, "sum": 0, "rate": 0.0},
        OBJ_RULE_TYPE_INDEX: {"violation_num": 0, "sum": 0, "rate": 0.0},
        # others
        RULE_TYPE_TEXT: {"violation_num": 0, "sum": 0, "rate": 0.0},
        RULE_TYPE_SQLSTAT: {"violation_num": 0, "sum": 0, "rate": 0.0},
        RULE_TYPE_SQLPLAN: {"violation_num": 0, "sum": 0, "rate": 0.0}
    }

    get_risk_rate.tik("start calc violation_num")

    record_id_set: set = set()
    for result in results_q:
        for rule_obj in rule_q:
            result_rule_dict = getattr(result, rule_obj.rule_name, None)
            if not result_rule_dict:
                continue
            if result.rule_type == RULE_TYPE_OBJ:
                records = result_rule_dict["records"]
                if rule_obj.obj_info_type in (OBJ_RULE_TYPE_TABLE, OBJ_RULE_TYPE_PART_TABLE):
                    ret["table"]["violation_num"] += len(records)
                elif rule_obj.obj_info_type == OBJ_RULE_TYPE_INDEX:
                    ret[OBJ_RULE_TYPE_INDEX]["violation_num"] += len(records)
                elif rule_obj.obj_info_type == OBJ_RULE_TYPE_SEQ:
                    ret[OBJ_RULE_TYPE_SEQ]["violation_num"] += len(records)
            else:
                sqls = result_rule_dict["sqls"]
                ret[result.rule_type]["violation_num"] += len(sqls)
        record_id_set.add(result.record_id)

    get_risk_rate.tik("start query mongo count")

    record_id_list = list(record_id_set)
    task_record_ids: list = list({int(i.split("##")[0]) for i in record_id_list})
    print(f"task_record_ids: {task_record_ids}")
    # table include normal table and part-table, so calc twice.
    ret["table"]["sum"] += tab_info_q.filter(record_id__in=record_id_list).count()
    ret["table"]["sum"] += part_tab_info_q.filter(record_id__in=record_id_list).count()
    # OBJ index
    ret[OBJ_RULE_TYPE_INDEX]["sum"] += index_q.filter(record_id__in=record_id_list).count()
    # OBJ sequence
    ret[OBJ_RULE_TYPE_SEQ]["sum"] += seq_q.filter(task_record_id__in=task_record_ids).count()
    # other
    ret[RULE_TYPE_TEXT]["sum"] += sql_text_q.filter(record_id__in=record_id_list).count()
    ret[RULE_TYPE_SQLSTAT]["sum"] += sql_stats_q.filter(record_id__in=record_id_list).count()
    ret[RULE_TYPE_SQLPLAN]["sum"] += sql_plan_q.filter(record_id__in=record_id_list).count()

    get_risk_rate.tik("end query mongo count")

    for k, v in ret.items():
        if v["sum"] and v["violation_num"]:
            v["rate"] = float(v["violation_num"]) / v["sum"]
    return ret


def get_white_list(session, cmdb_id) -> [dict]:
    """白名单信息"""
    return [i.to_dict() for i in WhiteListRules.filter_enabled(
        session, WhiteListRules.cmdb_id == cmdb_id)]
