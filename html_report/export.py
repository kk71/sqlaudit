import time
import tarfile

import os.path
from bson import ObjectId

import settings
from utils.datetime_utils import *
from utils import const
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
from task.mail_report import zip_file_path
from models.oracle import make_session,CMDB


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


def main_task(task_uuid,cmdb, page):
    """
    导出任务功能，获取任务id，匹配规则类型，生成离线页面等
    """
    result = MongoHelper.find_one("results", {"task_uuid": task_uuid})
    record_id = result['record_id']
    rule_type = result['rule_type'].upper()

    result = {k: v for k, v in result.items() if v and isinstance(v, dict)}
    if rule_type == const.RULE_TYPE_OBJ:
        result = {k: v for k, v in result.items() if v.get("records")}

    # result = format_sqlplan_result(result)

    if rule_type in (const.RULE_TYPE_SQLPLAN, const.RULE_TYPE_SQLSTAT):
        sql_plans = dict()
        for rule_name, rule_result_dict in result.items():
            for sql_dict in rule_result_dict.get("sqls", []):
                sql_id = sql_dict["sql_id"]
                hash_value = sql_dict["plan_hash_value"]
                key = (sql_id, str(hash_value), record_id)
                if key not in sql_plans.keys():
                    condition = {'SQL_ID': sql_id, 'PLAN_HASH_VALUE': int(hash_value), 'record_id': record_id}
                    sql_plans[key] = [x for x in MongoHelper.find('sqlplan', condition, {'_id': 0})]
                sql_dict['plan'] = sql_plans[key]

    schema=result['score']['schema_name']
    rule_type=result['score']['rule_type']
    host=cmdb.ip_address
    port=cmdb.port

    db_type = const.DB_ORACLE
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
    if rule_type.upper() == const.RULE_TYPE_OBJ:
        for rule_name, value in result.items():
            rules.append([rule_name, rule_summary[rule_name][0], len(value["records"]), value["scores"],
                          rule_summary[rule_name][3]])
    elif rule_type.upper() in const.ALL_RULE_TYPES_FOR_SQL_RULE:
        for rule_name, rule_result_dict in result.items():
            num = len(rule_result_dict.get("sqls", []))
            rules.append([
                rule_name,
                rule_summary[rule_name][0],
                num,
                rule_result_dict.get("scores", 0),
                rule_summary[rule_name][2]
            ])
    deduct_marks = 0
    for value in rules:
        deduct_marks += float(value[3])
    score = (total_score - deduct_marks) / total_score * 100

    print_html_body(page, str(host), str(port), str(schema))
    print_html_js(page)
    # print_html_chart(total_score, page, rules)
    print_html_rule_table(page, host, port, schema, rules,score)

    if rule_type == const.RULE_TYPE_OBJ:
        print_html_obj_detail_info(page, result, rules, rule_summary)
    else:
        print_html_rule_detail_table(page, result, rules, rule_type)
        if rule_type in (const.RULE_TYPE_SQLSTAT, const.RULE_TYPE_SQLPLAN):
            print_html_rule_detail_info(page, result, rules)
        elif rule_type == const.RULE_TYPE_TEXT:
            print_html_rule_text_detail_info(page, result, rules)


def export_task(job_ids:list)-> str:
    """
    生成报告的离线压缩包，可配合下载服务器使用
    """
    # MongoHelper.update_one("job", {'_id': job_id}, {'$set': {'exported': 1}})

    # The path to the generated file
    paths = "/tmp/" + str(int(time.time()))
    if not os.path.exists(paths):
        os.makedirs(paths)
    for job_id in job_ids:
        with make_session() as session:
            result = MongoHelper.find_one("results", {"task_uuid": job_id})
            cmdb=session.query(CMDB).filter_by(cmdb_id=result['cmdb_id']).first()
            file_name = result['sid'] + "_" + job_id + "_" + result['rule_type'] +\
                        "_" + datetime.now().strftime("%Y%m%d") + ".tar.gz"
            v_page = print_html_script('sqlreview report')
            main_task(job_id,cmdb, v_page)
            v_page.printOut(f"html_report/sqlreview.html")
            path = paths+"/" +file_name
            tar = tarfile.open(str(path), "w:gz")
            tar.add("html_report/css")
            tar.add("html_report/assets")
            tar.add("html_report/js")
            tar.add("html_report/sqlreview.html")
            tar.add("html_report/readme.txt")
            tar.close()
        # os.remove("task_export/sqlreview.html")

        # 文件生成完毕，状态export设置为True
        # Job.objects(id=job_id).update(set__exported=True)

    """
               packaging
               zip_file_path(
               The path to the generated file,
               The path to place the file,
               The name of the package file)"""
    file_path_list = [
        "export_sqlhealth_details_html",
        datetime.now().strftime("%Y%m%d%H%M%S") + ".zip"
    ]

    zipPath = zip_file_path(
        paths, settings.HEALTH_DIR, ''.join(file_path_list))

    return zipPath
