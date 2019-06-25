import tarfile

import os.path
import settings
from utils.datetime_utils import *

from .utils import print_html_script
from .utils import print_html_body
from .utils import print_html_js
from .utils import print_html_chart
from .utils import print_html_rule_table
from .utils import print_html_rule_detail_table
from .utils import print_html_rule_detail_info
from .utils import print_html_rule_text_detail_info
from .utils import print_html_obj_detail_info
from plain_db.mongo_operat import MongoHelper


def format_sqlplan_result(result):
    # include scores

    result = {k: v for k, v in result.items() if v and isinstance(v, dict)}
    for rule_name, items in result.items():

        break_rules = {}
        for key, values in items.items():
            new_key = '#'.join(key.split("#")[:2])
            break_rules[new_key] = values

        result[rule_name] = break_rules

    return result


def main_task(task_uuid, page):
    """
    导出任务功能，获取任务id，匹配规则类型，生成离线页面等
    """
    result = MongoHelper.find_one("results", {"task_uuid": task_uuid})
    record_id = result['record_id']
    rule_type = result['rule_type'].upper()

    result = {k: v for k, v in result.items() if v and isinstance(v, dict)}
    if rule_type == "OBJ":
        result = {k: v for k, v in result.items() if v.get("records")}

    # result = format_sqlplan_result(result)

    if rule_type in ["SQLPLAN", "SQLSTAT"]:
        sql_plans = dict()
        for rule_name, value in result.items():
            for sqlid_num in value:
                if sqlid_num == "scores":
                    continue
                sql_id, hash_value, *_ = sqlid_num.split("#")
                key = '#'.join([sql_id, str(hash_value), record_id])
                if key not in sql_plans:
                    condition = {'SQL_ID': sql_id, 'PLAN_HASH_VALUE': int(hash_value), 'record_id': record_id}
                    sql_plans[key] = [x for x in MongoHelper.find('sqlplan', condition, {'_id': 0})]
                value[sqlid_num]['plan'] = sql_plans[key]

    job_info = MongoHelper.find_one("job", {"id": task_uuid})
    host = job_info["desc"]["db_ip"]
    schema = job_info["desc"]["owner"]
    rule_type = job_info["desc"]["rule_type"].upper()
    port = int(job_info["desc"]["port"])
    db_type = "O" if port == 1521 else "mysql"
    mongo_rules = [x for x in MongoHelper.find("rule", {"rule_type": rule_type})]

    total_score = sum([float(data["max_score"]) for data in [x for x in mongo_rules if x['db_type'] == db_type]])
    rule_summary = {}
    for rule in mongo_rules:
        values = [rule["rule_summary"], rule["exclude_obj_type"]]
        if rule_type == "OBJ":
            values.append(rule["output_parms"])
        values.append("<br>".join(rule["solution"]))
        rule_summary[rule["rule_name"]] = values

    rules = []
    if rule_type.upper() == "OBJ":
        for rule_name, value in result.items():
            rules.append([rule_name, rule_summary[rule_name][0], len(value["records"]), value["scores"],
                          rule_summary[rule_name][3]])
    elif rule_type.upper() in ["SQLPLAN", "SQLSTAT", "TEXT"]:
        for rule_name, value in result.items():
            num = sum([1 for x in result[rule_name] if "#" in x])
            rules.append(
                [rule_name, rule_summary[rule_name][0], num, value.get("scores", 0), rule_summary[rule_name][2]])

    print_html_body(page, str(host), str(port), str(schema))
    print_html_js(page)
    print_html_chart(total_score, page, rules)
    print_html_rule_table(page, host, port, schema, rules)

    if rule_type == "OBJ":
        print_html_obj_detail_info(page, result, rules, rule_summary)
    else:
        print_html_rule_detail_table(page, result, rules, rule_type)
        if rule_type in ["SQLPLAN", "SQLSTAT"]:
            print_html_rule_detail_info(page, result, rules)
        elif rule_type == "TEXT":
            print_html_rule_text_detail_info(page, result, rules)


def export_task(job_id):
    """
    生成报告的离线压缩包，可配合下载服务器使用
    """
    # MongoHelper.update_one("job", {'_id': job_id}, {'$set': {'exported': 1}})

    result = MongoHelper.find_one("results", {"task_uuid": job_id})
    file_name = result['sid'] + "_" + job_id + "_" + result['rule_type'] + "_" + datetime.now().strftime("%Y%m%d")

    v_page = print_html_script()
    main_task(job_id, v_page)
    v_page.printOut("html_report/sqlreview.html")
    path = os.path.join(settings.EXPORT_DIR, file_name + ".tar.gz")
    tar = tarfile.open(str(path), "w:gz")
    tar.add("html_report/css")
    tar.add("html_report/assets")
    tar.add("html_report/js")
    tar.add("html_report/sqlreview.html")
    tar.add("html_report/readme.txt")
    tar.close()
    # os.remove("task_export/sqlreview.html")

    # 文件生成完毕，状态export设置为True
    MongoHelper.update_one("job", {'_id': job_id}, {'$set': {'exported': True}})
