# -*- coding: utf-8 -*-

import os
import re
import json
import time
import chardet
import xlsxwriter
import sqlparse
import random
import string
from decimal import Decimal
from functools import wraps
from datetime import datetime
from collections import defaultdict

import arrow

import settings
import cx_Oracle
from plain_db.oracleob import OracleOB, OracleHelper
from plain_db.mongo_operat import MongoHelper
from .cmdb_utils import CmdbUtils
import utils.const

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))


def check_file_encoding(file_stream):
    encoding_check = chardet.detect(file_stream)
    if float(encoding_check['confidence']) >= 0.65:
        encoding = encoding_check['encoding']
        encoding = 'gbk' if encoding == 'KOI8-R' else encoding
    else:
        encoding = 'gbk'

    return encoding


def filter_annotation(sql):
    # sql = sql.strip()
    # prefixes = ["DELETE", "INSERT", "UPDATE", "MERGE", "SELECT"]
    # if all([not re.search(prefix + "\s*(/\*\+.*?\*/)", sql, re.I) for prefix in prefixes]) and re.search("(/\* \+.*?\*/)", sql, re.I):
    #     sql = re.sub("(/\*.*?\*/)", "", sql)
    if not sql:
        return ""
    # sql = '\n'.join([x.strip() for x in sql.split('\n') if not re.search("^(remark|--)", x.strip(), re.I)])
    # sql = re.sub('(--.*?\n)', "", sql)
    # sql = re.sub('(--.*?)$', "", sql)
    # sql = re.sub('\/\* +.*?\*\/', "", sql)
    # sql = re.sub('\/\*[^+]{2}[\s\S]*?\*\/\n', "", sql)
    # sql = re.sub('\n', " ", sql)

    sql = sql[:-1] if sql and sql[-1] == ";" else sql

    return sql.strip()

def annotation_condition(sql):
    sql = sql.strip()

    if not sql:
        return True

    if re.match("^\s*(remark|rem|--|\/\*)\s+", sql, re.I):
        return True

    if re.match('\s*--.*?', sql, re.I):
        return True

    if re.match('\/\* +.*?\*\/', sql, re.I):
        return True
    if re.match('\/\*[^+]{2}[\s\S]*?\*\/\n', sql, re.I):
        return True

    return False

def is_annotation(sql):

    multi_annotation = re.findall("\/\*[\s\S]*?\*\/", sql, re.I)
    for anno in multi_annotation:
        sql = sql.replace(anno, "")
    return all([annotation_condition(x) for x in sql.split("\n")])

def get_risk_obj_body(cmdb_id, db_model, obj_rules, owner_list, query_start, query_end):
    condition = {
        'rule_type': "OBJ",
        'rule_name': {"$in": [x for x in obj_rules]},
        'db_model': db_model
    }
    orig_rules = {x['rule_name']: x for x in MongoHelper.find("rule", condition, {'_id': 0})}
    condition = {
        'rule_type': "OBJ",
        'cmdb_id': cmdb_id,
        'username': {"$in": owner_list},
        'create_time': {"$gte": query_start, "$lt": query_end},
    }

    results = MongoHelper.find("results", condition, {"_id": 0, "task_uuid": 0})
    risk_obj_dict = format_obj_results(results, obj_rules)
    risk_obj_result = []
    for rule_name, risk_objs in risk_obj_dict.items():
        sql = [
            {"$match": {
                "$and": [
                    {rule_name + ".records": {
                        "$exists": True
                    }},
                    {rule_name + ".records": {
                        "$not": {
                            "$size": 0
                        }
                    }}
                ]
            }},
            {"$group": {
                '_id': rule_name,
                "first_appear": {"$min": "$create_time"},
                "last_appear": {"$max": "$create_time"}
            }},
            {"$project": {
                '_id': 0,
                'first_appear': 1,
                'last_appear': 1
            }}
        ]
        appears = [x for x in MongoHelper.aggregate("results", sql)]
        first_appear = appears[0]['first_appear'] if appears else ""
        last_appear = appears[0]['last_appear'] if appears else ""
        for risk_obj in risk_objs[:-1]:
            obj_name = risk_obj[0]
            output_params = orig_rules[rule_name]['output_parms']
            risk_detail = ', '.join([': '.join([output_params[index]['parm_desc'], str(value)]) for index, value in enumerate(risk_obj)])
            risk_name = orig_rules[rule_name]['rule_desc']
            solution = ', '.join(orig_rules[rule_name]['solution'])
            risk_obj_result.append(
                ['<input type="checkbox" name="checktd">', obj_name, risk_name, risk_detail, first_appear[:-3], last_appear[:-3], solution])
    return risk_obj_result


def item_not_in_list(items, list_items):
    for item in items:
        if item_not_in_lists(item, list_items) is False:
            return False
    return True


def item_not_in_lists(item, list_items):
    for list_item in list_items:
        for index in range(len(list_item)):
            if list_item[index] != item[index]:
                return False
    return True


def format_obj_results(results, rule_list) -> dict:
    set_dict = defaultdict(list)
    rule_name2create_time = {}
    for result in results:
        for rule_name, records in result.items():
            if rule_name not in rule_list or not isinstance(records, dict) or not records['records']:
                continue
            for record in records['records']:
                if item_not_in_list(record, set_dict[rule_name]):
                    set_dict[rule_name] += records['records']
            rule_name2create_time[rule_name] = result['create_time']
    for rule_name in set_dict:
        set_dict[rule_name].append(rule_name2create_time[rule_name])
    return set_dict


def format_results(results, rule_list, key='rule_name') -> dict:
    set_dict = defaultdict(set)
    for result in results:
        for rule_name, sql_ids in result.items():
            if rule_name not in rule_list or not sql_ids or 'records' in sql_ids:
                continue
            if key == 'rule_name':
                [set_dict[rule_name].add(kvalue['sql_id']) for krisk, kvalue in sql_ids.items() if krisk != 'scores']
            elif key == 'schemas':
                [set_dict[result['username']].add(kvalue['sql_id']) for krisk, kvalue in sql_ids.items() if krisk != 'scores']

    return set_dict


def format_data(data):
    if isinstance(data, datetime):
        return data.strftime("%Y-%m-%d %X")
    if data is None:
        return ""
    return data


def render(template_name):
    def wrapper(func):
        @wraps(func)
        def _wrapper(self, *args, **kwargs):
            params = func(self, *args, **kwargs) or {}
            login_user = self.get_current_user()
            if login_user not in self.users:
                sql = "SELECT user_name FROM T_USER WHERE login_user = :1"
                res = self.odb.select(sql, login_user, one=True)
                if res:
                    self.users[login_user] = res['user_name']
                    user_name = res['user_name']
                else:
                    user_name = "Not a Valid user"
            else:
                user_name = self.users[login_user]

            params.update({
                'username': user_name,
                'auth': self.get_auth(),
                'nav': params.get("nav", "sysconfig")
            })
            return self.render(template_name, **params)

        return _wrapper

    return wrapper


def get_time(timestamp=None, format=None, return_str=False):
    timestamp = timestamp or time.time()
    format = format or "%Y-%m-%d %X"
    if return_str:
        return time.strftime(format, time.localtime(timestamp))
    else:
        return arrow.get(time.localtime(timestamp)).datetime



def get_random_str(length=10):
    lib = string.digits + string.ascii_lowercase
    return ''.join([random.choice(lib) for _ in range(length)])


def mktime(str_time, format=None):
    format = format or "%Y-%m-%d"
    return int(time.mktime(time.strptime(str_time, format)))


class CustomEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if o is None:
            return ""
        if isinstance(o, datetime):
            return o.strftime("%Y-%m-%d %X")
        super().default(o)


def jl(data):
    return json.loads(data)


def jd(data):
    return json.dumps(data, cls=CustomEncoder)


def result_icon(result):
    return {
        0: '<i class="fa fa-times" style="color:red" aria-hidden="true"></i>',
        1: '<i class="fa fa-check" style="color:green" aria-hidden="true"></i>',
        2: "手动停止",
    }.get(result, """<a href="javascript:stopTask('{task_uuid}');">停止</a>""")


def calculate_rules(result, search_temp):

    rule_type = result['rule_type']
    cmdb_id = result['cmdb_id']
    cmdb = CmdbUtils.get_cmdb(cmdb_id)
    db_model = cmdb['db_model']

    result = {k: v for k, v in result.items() if v and isinstance(v, dict)}

    rule_infos = [x for x in MongoHelper.find("rule", {'rule_type': rule_type,
                                                       'db_type': utils.const.DB_ORACLE,
                                                       'db_model': db_model})]
    rule_summary = {v['rule_name']: [v['rule_summary'], v['exclude_obj_type']] for v in rule_infos}
    total_score = sum([float(rule['max_score']) for rule in rule_infos])

    rules = []
    if rule_type == "OBJ":
        for key, value in result.items():
            if not value.get("records"):
                continue
            temp_set = [temp[0] for temp in value["records"]]
            search_temp.update(
                {"OBJECT_TYPE": rule_summary[key][1]}
            )
            # prevent object
            prevent_obj = MongoHelper.find("exclude_obj_info", search_temp)
            prevent_temp = [data["OBJECT_NAME"] for data in prevent_obj]
            final_set = list(set(temp_set) - set(prevent_temp))
            if not final_set:
                continue
            weighted_score = round(float(value["scores"]) * 100 / (total_score or 1), 2)
            rules.append([key, rule_summary[key][0], len(final_set), round(float(value["scores"]) / 1.0, 2), weighted_score])
    else:
        for rule_name, value in result.items():
            risk_sql_num = sum([1 for x in value if '#' in x])
            weighted_score = round(float(value["scores"]) * 100 / (total_score or 1), 2)
            rules.append([rule_name, rule_summary[rule_name][0], risk_sql_num, round(float(value["scores"]) / 1.0, 2), weighted_score])

    marks = sum([float(x[3]) for x in rules])

    if total_score == 0:
        print(f"total score is 0!!! {result}")
        scores_total = 0
    else:
        scores_total = round((total_score - marks) / total_score * 100 or 1, 2)
    real_score = scores_total if scores_total > 40 else 40

    return rules, real_score


def create_sql_xlsx(filename, fields, data):
    path = os.path.join(ROOT_PATH, 'webui/static', filename)
    wb = xlsxwriter.Workbook(path)
    format_title = wb.add_format({
        'bold': 1,
        'size': 14,
        'align': 'center',
        'valign': 'vcenter',

    })
    format_text = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
    })
    format_top = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
        'color': 'red',
        'bg_color': 'yellow'
    })

    average_ws = wb.add_worksheet('风险sql报告 平均值降序')
    total_ws = wb.add_worksheet('风险sql报告 总值降序')

    average_data = data['average_data']
    total_data = data['total_data']

    fill_up_data(average_ws, average_data, fields, format_title, format_text, format_top)
    fill_up_data(total_ws, total_data, fields, format_title, format_text, format_top)

    wb.close()

    return path


def fill_up_data(ws, data, fields, format_title, format_text, format_top):
    ws.set_column(0, 2, 20)
    ws.set_column(3, 4, 18)
    ws.set_column(5, 5, 10)
    ws.set_column(6, 8, 18)
    ws.set_column(9, 9, 50)
    ws.set_row(0, 30)

    [ws.write(0, x, field.upper(), format_title) for x, field in enumerate(fields)]
    for row_num, row in enumerate(data):
        sqltext = sqlparse.format(row[-1], reindent=True, keyword_case='upper')
        ws.write_row(row_num + 1, 0, row[:3], format_text)
        ws.write(row_num + 1, 3, row[3], format_text)
        ws.write(row_num + 1, 4, row[4], format_text)
        ws.write(row_num + 1, 5, row[5], format_text)
        ws.write(row_num + 1, 6, row[6], format_text)
        ws.write(row_num + 1, 7, row[7], format_text)
        ws.write(row_num + 1, 8, row[8], format_text)
        ws.write(row_num + 1, 9, sqltext, format_text)


def create_sqlhealthy_xlsx(filename, data):
    path = os.path.join(ROOT_PATH, 'webui/static', filename)
    wb = xlsxwriter.Workbook(path)
    format_title = wb.add_format({
        'bold': 1,
        'size': 14,
        'align': 'center',
        'valign': 'vcenter',

    })
    format_text = wb.add_format({
        'valign': 'vcenter',
        'align': 'center',
        'size': 14,
        'text_wrap': True,
    })

    heads = data['heads']
    heads_data = data['heads_data']
    excel_data_dict = data['excel_data_dict']

    for rule_key, rule_value in excel_data_dict.items():

        rule_heads = rule_value['rule_heads']
        rule_data = rule_value['rule_data']
        solution = rule_value['solution']
        records = rule_value['records']
        table_title = rule_value['table_title']

        rule_ws = wb.add_worksheet(rule_key)

        rule_ws.set_column(0, 0, 40)
        rule_ws.set_column(1, 1, 110)
        rule_ws.set_column(2, 2, 30)
        rule_ws.set_column(3, 6, 30)

        [rule_ws.write(0, x, field, format_title) for x, field in enumerate(heads)]
        [rule_ws.write(1, x, field, format_text) for x, field in enumerate(heads_data)]

        [rule_ws.write(3, x, field, format_title) for x, field in enumerate(rule_heads)]
        [rule_ws.write(4, x, field, format_text) for x, field in enumerate(rule_data)]

        [rule_ws.write(6, x, field, format_title) for x, field in enumerate(table_title)]

        num = 1
        for records_data in records:
            [rule_ws.write(6 + num, x, field, format_text) for x, field in enumerate(records_data)]
            num += 1

        last_num = 6 + len(records) + 2

        last_data = ['修改意见: ', solution]
        [rule_ws.write(last_num, x, field, format_title) for x, field in enumerate(last_data)]

    wb.close()

    return path


def create_worklist_xlsx(filename, parame_dict):

    path = os.path.join(settings.EXPORT_DIR, filename)
    wb = xlsxwriter.Workbook(path)
    ws = wb.add_worksheet('任务工单详情')
    ws.set_column(0, 2, 20)
    ws.set_column(3, 4, 18)
    ws.set_column(5, 6, 20)
    ws.set_column(6, 8, 18)
    ws.set_column(9, 9, 50)
    ws.set_row(0, 30)
    format_title = wb.add_format({
        'bold': 1,
        'size': 14,
        'align': 'center',
        'valign': 'vcenter',

    })
    format_text = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
    })
    format_top = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
        'color': 'red',
        # 'bg_color': 'yellow'
    })
    works_heads = parame_dict['work_list_heads']
    [ws.write(0, x, field.upper(), format_title) for x, field in enumerate(works_heads)]

    works_data = parame_dict['work_list_data']
    ws.write(1, 0, works_data[0], format_text)
    ws.write(1, 1, works_data[1], format_text)
    ws.write(1, 2, works_data[2], format_text)
    ws.write(1, 3, works_data[3], format_text)
    ws.write(1, 4, works_data[4], format_text)
    ws.write(1, 5, works_data[5], format_text)
    ws.write(1, 6, works_data[6], format_text)
    ws.write(1, 7, works_data[7], format_text)
    ws.write(1, 8, works_data[8], format_text)
    ws.write(1, 9, works_data[9], format_text)
    ws.write(1, 10, works_data[10], format_text)
    ws.write(1, 11, works_data[11], format_text)
    ws.write(1, 12, works_data[12], format_text)
    ws.write(1, 13, works_data[13], format_text)
    ws.write(1, 14, works_data[14], format_text)
    ws.write(1, 15, works_data[18], format_text)

    # 同一个sheet里面 统计工单数量
    fail_data = parame_dict['fail_data']
    fail_heads = parame_dict['fail_heads']
    [ws.write(3, x, field.upper(), format_title) for x, field in enumerate(fail_heads)]
    ws.write(4, 0, fail_data[0], format_text)
    ws.write(4, 1, fail_data[1], format_top)
    ws.write(4, 2, fail_data[2], format_text)
    ws.write(4, 3, fail_data[3], format_text)

    # 创建新的sheet 静态失败SQL
    static_ws = wb.add_worksheet('静态失败SQL')
    static_ws.set_column(0, 1, 25)
    static_ws.set_column(1, 3, 70)

    static_fail_heads = parame_dict['static_fail_heads']
    static_fail_data = parame_dict['static_fail_data']
    [static_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(static_fail_heads)]
    for row_num, row in enumerate(static_fail_data):
        static_ws.write(row_num + 1, 0, row[0], format_text)
        static_ws.write(row_num + 1, 1, row[1], format_text)
        static_ws.write(row_num + 1, 2, row[2], format_text)

    # 创建新的sheet 静态失败SQL
    dynamic_ws = wb.add_worksheet('动态失败SQL')
    dynamic_ws.set_column(0, 1, 25)
    dynamic_ws.set_column(1, 3, 70)

    dynamic_fail_heads = parame_dict['dynamic_fail_heads']
    dynamic_fail_data = parame_dict['dynamic_fail_data']

    [dynamic_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(dynamic_fail_heads)]
    for row_num, row in enumerate(dynamic_fail_data):
        dynamic_ws.write(row_num + 1, 0, row[0], format_text)
        dynamic_ws.write(row_num + 1, 1, row[1], format_text)
        dynamic_ws.write(row_num + 1, 2, row[2], format_text)

    # 创建新的sheet 所有检测SQL
    all_ws = wb.add_worksheet('所有检测SQL')
    all_ws.set_column(0, 1, 25)
    all_ws.set_column(1, 4, 70)
    all_work_heads = parame_dict['all_work_heads']
    all_work_data = parame_dict['all_work_data']

    [all_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(all_work_heads)]
    for row_num, row in enumerate(all_work_data):
        all_ws.write(row_num + 1, 0, row[0], format_text)
        all_ws.write(row_num + 1, 1, row[1], format_text)
        all_ws.write(row_num + 1, 2, row[2], format_text)
        all_ws.write(row_num + 1, 3, row[3], format_text)

    wb.close()
    return path


def create_obj_xlsx(filename, fields, data):
    path = os.path.join(ROOT_PATH, 'webui/static', filename)
    wb = xlsxwriter.Workbook(path)
    ws = wb.add_worksheet('风险对象报告')
    title_format = wb.add_format({
        'size': 14,
        'bold': 1,
        'align': 'center',
        'valign': 'vcenter',
    })
    content_format = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
    })
    ws.set_row(0, 20, title_format)
    ws.set_column(0, 0, 18)
    ws.set_column(1, 1, 20)
    ws.set_column(2, 2, 40)
    ws.set_column(3, 4, 16)
    ws.set_column(5, 5, 50)
    [ws.write(0, x, field, title_format) for x, field in enumerate(fields)]
    for row_num, row in enumerate(data):
        ws.write_row(row_num + 1, 0, row[:3], content_format)
        ws.write(row_num + 1, 3, row[3], content_format)
        ws.write(row_num + 1, 4, row[4], content_format)
        ws.write(row_num + 1, 5, row[5], content_format)
    wb.close()
    return path


def create_subwork_xlsx(filename, fields, data):
    path = os.path.join(ROOT_PATH, 'webui/static', filename)
    wb = xlsxwriter.Workbook(path)
    ws = wb.add_worksheet('子工单报告')
    ws.set_column(0, 2, 20)
    ws.set_column(3, 4, 18)
    ws.set_column(5, 5, 10)
    ws.set_column(6, 6, 50)
    ws.set_row(0, 30)
    format_title = wb.add_format({
        'bold': 1,
        'size': 14,
        'align': 'center',
        'valign': 'vcenter',

    })
    format_text = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
    })

    [ws.write(0, x, field.upper(), format_title) for x, field in enumerate(fields)]
    for row_num, row in enumerate(data):
        # sqltext = sqlparse.format(row[-1], reindent=True, keyword_case='upper')
        ws.write_row(row_num + 1, 0, row[:3], format_text)
        ws.write(row_num + 1, 3, row[3], format_text)
        ws.write(row_num + 1, 4, row[4], format_text)
        ws.write(row_num + 1, 5, row[5], format_text)
        ws.write(row_num + 1, 6, row[6], format_text)
        ws.write(row_num + 1, 7, row[7], format_text)
        ws.write(row_num + 1, 8, row[8], format_text)
        ws.write(row_num + 1, 9, row[9], format_text)
        ws.write(row_num + 1, 10, row[10], format_text)
        ws.write(row_num + 1, 11, row[11], format_text)
        ws.write(row_num + 1, 12, row[12], format_text)
        # ws.write(row_num + 1, 6, sqltext, format_text)
    wb.close()
    return path


class IllegalJsonBody(Exception):
    pass


class StopCeleryException(Exception):
    pass


class CustomInvalid(Exception):
    pass


class TestValidator(object):

    def __init__(self, msg):
        self.msg = msg

    def __call__(self, value):
        if value != "is_test_user":
            raise CustomInvalid(self.msg or 'self defined msg')
        return value

    def __repr__(self):
        return "This is a test validator"


class TransError:
    user_add = {
        'login_user': "登录名必须大于三位数",
        'user_name': "用户名必须大于三位数",
        'password': "密码必须大于六位数",
        'mobile_phone': "电话号码必须为11位",
    }

    add_host = {
        'connect_name': "连接名字不能为空",
        'group_name': "组名不能为空",
        'business_name': "业务系统不能为空",

        'machine_room': "机房不能为空",
        'database_type': "数据库类型不能为空",
        'server_name': "主机名不能为空",

        'ip_address': "IP地址不能为空",
        'port': "端口不能为空",
        'service_name': "服务名不能为空",

        'user_name': "用户名不能为空",
        'password': "密码不能为空",
        'is_collect': "是否采集不能为空",
        'status': "是否上线不能为空",
    }

    edit_host = {
        'cmdb_id': "请选择数据库主机",
        'connect_name': "连接名字不能为空",
        'group_name': "组名不能为空",
        'business_name': "业务系统不能为空",

        'machine_room': "机房不能为空",
        'database_type': "数据库类型不能为空",
        'server_name': "主机名不能为空",

        'ip_address': "IP地址不能为空",
        'port': "端口不能为空",
        'service_name': "服务名不能为空",

        'user_name': "用户名不能为空",
        'password': "密码不能为空",
        'is_collect': "是否采集不能为空",
        'status': "是否上线不能为空",
        'domain_env': "所属域",
        'auto_sql_optimized': "是否自动化不能为空",
    }

    add_task = {
        'cmdb_id': "请选择数据库主机",
        'task_is_collect': "启用状态不能为空",
        'add_task_schedule': "任务脚本不能为空",
        'add_task_scripts': "任务时间不能为空",
    }

    add_rule = {
        'risk_name': "风险名字不能为空",
        'risk_demo': "风险维度不能为空",
        'rule_severity': "严重等级不能为空",
        'rule_name': "规则名字不能为空",
        'optimized_advice': "优化建议不能为空",
    }

    email_rule = {
        'email_title': "标题不能为空",
        'email_contents': "内容不能为空",
        'email_sender': "发件人不能为空",
        'sending_date': "发送日期不能为空",
        'time_point': "发送时间点不能为空",
        'send_content_item': "邮件类型不能为空",
    }

    server_rule = {
        'server_name': "邮件服务器名称",
        'ip_address': "IP地址",
        'protocol_name': "协议名称",
        'port': "端口号",
        'username': "用户名",
        'password': "密码",
        'status': "状态",
        'remarks': "备注",
        'usessl': "启用SSL",
    }

    modules = {
        'user_add': user_add,
        'add_task': add_task,
        'add_host': add_host,
        'edit_host': edit_host,
        'add_rule': add_rule,
        'email_rule': email_rule,
        'server_rule': server_rule,
    }

    @classmethod
    def exe(cls, module_name, errs):
        trans = cls.modules[module_name]
        err_list = []
        for err in errs.split('\n'):
            [err_list.append(trans[x]) for x in trans if x in err]
        return '\n'.join(err_list)


AddHostError = TransError
AddTasktError = TransError
EditHostError = TransError

GET_USER_SQL = """
select distinct owner from (
    SELECT /*+ parallel( a 4) */ OWNER,count(*) count1
    FROM dba_tables  a
    WHERE owner  NOT IN
    (
     'SYS', 'OUTLN', 'SYSTEM', 'CTXSYS', 'DBSNMP',
     'LOGSTDBY_ADMINISTRATOR', 'ORDSYS',
     'ORDPLUGINS', 'OEM_MONITOR', 'WKSYS', 'WKPROXY',
     'WK_TEST', 'WKUSER', 'MDSYS', 'LBACSYS', 'DMSYS',
     'WMSYS', 'OLAPDBA', 'OLAPSVR', 'OLAP_USER',
     'OLAPSYS', 'EXFSYS', 'SYSMAN', 'MDDATA',
     'SI_INFORMTN_SCHEMA', 'XDB', 'ODM','DBA','FLOWS_030000','DBMON','TIVOLI','FLOWS_FILES','OWBSYS',
      'APEX_050100','SQLAUTID','ANONYMOUS','SCOTT',
     'SQLAUD','ADMIN','APPQOSSYS','ORDDATA','ANONYMOUS','SQLAUDIT')   group by owner having count(*) >=1
    union
    select /*+ parallel(4) */ parsing_schema_name owner,count(*) count1 from dba_hist_sqlstat where
    parsing_schema_name NOT IN
    (
     'SYS', 'OUTLN', 'SYSTEM', 'CTXSYS', 'DBSNMP',
     'LOGSTDBY_ADMINISTRATOR', 'ORDSYS',
     'ORDPLUGINS', 'OEM_MONITOR', 'WKSYS', 'WKPROXY',
     'WK_TEST', 'WKUSER', 'MDSYS', 'LBACSYS', 'DMSYS',
     'WMSYS', 'OLAPDBA', 'OLAPSVR', 'OLAP_USER',
     'OLAPSYS', 'EXFSYS', 'SYSMAN', 'MDDATA',
     'SI_INFORMTN_SCHEMA', 'XDB', 'ODM','DBA','FLOWS_030000','DBMON','TIVOLI','APEX_050100','SQLAUTID','ANONYMOUS','SCOTT',
     'SQLAUD','ADMIN','APPQOSSYS','ORDDATA','ANONYMOUS','SQLAUDIT','FLOWS_FILES','OWBSYS'
     )
     group by parsing_schema_name having count(*) >=1   )  aa order by 1 asc
"""
