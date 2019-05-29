# -*- coding: utf-8 -*-
import os
import re
import xlrd
import string
import random
import logging
from datetime import datetime

import sqlparse

from .utils import ROOT_PATH
from plain_db.mongo_operat import MongoHelper
from .rule_utils import RuleUtils
from plain_db.oracleob import OracleOB
from plain_db.oracleob import OracleHelper
from .sql_parse_rules import ObjStaticRules
from utils.sql_utils import SQL_DDL as DDL
from .constant import worklist_type_static_rule
from .constant import worklist_type_dynamic_sqlplan_rule
from .constant import RULE_COMMANDS
from .utils import is_annotation
from .constant import CREATE_APP_SQL, CREATE_DBA_SQL, OTHER_SQL, SQLPLUS_SQL


class Check:

    @classmethod
    def detect_sqlplus_command(cls, sql):

        if re.match(f"^\s*({'|'.join(SQLPLUS_SQL)})(\s+.*?)?\s*$", sql, re.I):
            return True

        if sql.strip().startswith("@"):
            return True

    @classmethod
    def detect_other_sql(cls, sql):

        if re.match(f"\s*({'|'.join(OTHER_SQL)})\s*;?", sql, re.I):
            return True

    @classmethod
    def detect_create_app_sql(cls, sql):
        for line in CREATE_APP_SQL:
            if re.match(f"\s*{line[0]}\s+({line[1]}\s+)?({line[2]}\s+)?({line[3]}\s+)?{line[4]}[\s\S]*[;\/]?", sql, re.I):
                return True
        return False

    @classmethod
    def detect_create_dba_sql(cls, sql):
        for line in CREATE_DBA_SQL:
            if re.match(f"\s*{line[0]}\s+({line[1]}\s+)?({line[2]}\s+)?{line[3]}[\s\S]*[;]?", sql, re.I):
                return True
        return False

    @classmethod
    def eliminate_annotation(cls, sql):
        multi_annotation = re.findall("\/\*[\s\S]*?\*\/", sql, re.I)
        for anno in multi_annotation:
            sql = sql.replace(anno, "")
        return '\n'.join([row for row in sql.split("\n") if not re.match("^(--|rem)", row.strip(), re.I)])

    @classmethod
    def is_explain_unvalid_sql(cls, sql):
        if re.search(r'\s*(set\s+\w+\s+\w+)', sql, re.I):
            return True

        if re.search(r'\s*(alter|drop)\s+table', sql, re.I):
            return True

        if re.search(r'\s*(declare|replace\s+procedure)', sql, re.I):
            return True

        sql = cls.eliminate_annotation(sql)

        if cls.detect_other_sql(sql) is True:
            return True

        if cls.detect_create_app_sql(sql) is True:
            return True

        if cls.detect_create_dba_sql(sql) is True:
            return True

        if cls.detect_sqlplus_command(sql) is True:
            return True

        return False

    @classmethod
    def text_parse(cls, key, rule_complexity, rule_cmd, input_params, sql):

        args = {param["parm_name"]: param["parm_value"] for param in input_params}

        violate = False
        # 解析简单规则
        if rule_complexity == "simple" and re.search(rule_cmd, sql):
            violate = True
        elif rule_complexity == "complex":
            module_name = ".".join(["rule_analysis.rule.text", key.lower()])
            module = __import__(module_name, globals(), locals(), "execute_rule")
            if module.execute_rule(sql=sql, **args):
                violate = True

        return violate

    @classmethod
    def sqlplan_parse(cls, record_id, rule_name, params):

        tmp0, tmp1 = RuleUtils.gen_random_collection()

        rule_cmd = RULE_COMMANDS.get(rule_name, params['rule_cmd'])
        for parm in params['input_parms']:
            rule_cmd = rule_cmd.replace("@" + parm["parm_name"] + "@", str(parm["parm_value"]))

        rule_cmd = rule_cmd.replace("@username@", params['username']).replace("@sql@", 'sqlplan').\
            replace("@tmp@", tmp0).replace("@tmp1@", tmp1).\
            replace("\"@etl_date_key@\"", "\"record_id\"").replace("\"@etl_date@\"", "\"%s\"" % record_id).\
            replace("@record_id@", "%s" % record_id).replace("@ip_addr@", params['ip_addr']).replace("@sid@", params['sid'])

        # if rule_name in ["LOOP_IN_TAB_FULL_SCAN"]:
        #     print(rule_name)
        MongoHelper.drop(tmp0)
        MongoHelper.drop(tmp1)

        MongoHelper.command(rule_cmd)
        records = [x for x in MongoHelper.find(tmp0, {})]

        MongoHelper.drop(tmp0)
        MongoHelper.drop(tmp1)

        if not records:
            return False

        return True

    @classmethod
    def parse_single_sql(cls, sql, worklist_type, db_model):

        formatted_sql = sqlparse.format(sql, strip_whitespace=True).lower()
        parse_results = {}

        for rule_name, value in RuleUtils.text(db_model).items():
            if rule_name in worklist_type_static_rule[worklist_type]:
                try:
                    err = cls.text_parse(
                        rule_name,
                        value["rule_complexity"],
                        value["rule_cmd"],
                        value["input_parms"],
                        formatted_sql
                    )
                    parse_results[rule_name] = err
                except Exception as err:
                    parse_results[rule_name] = str(err)
                    logging.error("Exception:", exc_info=True)

        violet_obj_rules = ObjStaticRules.run(sql, db_model) if worklist_type == DDL else ""
        err_msgs = [RuleUtils.rule_info()[rule_name]['rule_desc'] for rule_name in parse_results if parse_results[rule_name]] + [violet_obj_rules]
        return "" if not err_msgs else '\n'.join(err_msgs)

    @classmethod
    def parse_sql(cls, sqls):
        return_msgs = [cls.parse_single_sql(sql.lower()) for sql in sqls.split(";") if sql]
        return return_msgs

    @classmethod
    def get_random_str(cls):
        lib = string.digits + string.ascii_lowercase

        while True:
            random_str = ''.join([random.choice(lib) for _ in range(15)])
            sql = "SELECT COUNT(*) FROM T_SQL_PLAN WHERE statement_id = :1"
            if OracleHelper.select(sql, [random_str])[0] == 0:
                break

        return random_str

    @classmethod
    def parse_excel(cls, filename):
        filepath = os.path.join(ROOT_PATH, "files", filename)
        excel = xlrd.open_workbook(filepath)
        sheet = excel.sheet_by_index(0)
        if sheet.nrows <= 3:
            return []

        system_name = sheet.row_values(0)[1]
        database_name = sheet.row_values(0)[1]
        return [[x for x in sheet.row_values(row)[:2]] for row in range(3, sheet.nrows)], system_name, database_name

    @classmethod
    def get_procedures_end_with_slash(cls, sql_contents):

        cate = ["declare", "create\s+(?:or\s+replace\s+)?(?:EDITIONABLE|NONEDITIONABLE\s+)?(?:FUNCTION|PACKAGE|PACKAGE BODY|PROCEDURE|TRIGGER|TYPE BODY)"]
        re_str = f"\n(\s*set\s+\w+\s+\w+)|\n\s*((?:{'|'.join(cate)})[\s\S]*?end;[\s\S]*?\/)|\n\s*((?:{'|'.join(SQLPLUS_SQL)})(?:\s+.*?)?)\n|\n\s*(@@?.*?)\n"
        procedures = [''.join(x) for x in re.findall(re_str, sql_contents, re.I)]
        return procedures

    @classmethod
    def parse_sql_file(cls, sql_contents, sql_keyword):

        # sql_keyword doesn't used.

        procedures = cls.get_procedures_end_with_slash(sql_contents)
        for procedure in procedures:
            sql_contents = sql_contents.replace(procedure, "|||||")

        sql_contents = [x.strip(' ') for x in sql_contents.split("|||||")]

        sql_list = []
        for index, content in enumerate(sql_contents):
            sql_list += [a for b in [sqlparse.split(x) for x in content.split(';')] for a in b]
            if index < len(procedures) and procedures[index].strip():
                sql_list.append(procedures[index].strip())

        sql_list = [sql for sql in sql_list if sql]

        new_sql_list = []
        annotation_sql = ""
        for sql in sql_list:

            if is_annotation(sql):
                annotation_sql += sql
            else:
                new_sql_list.append((annotation_sql + "\n" + sql).lstrip().replace('\n\n', '\n').replace('\n', '<br/>').replace("\"", "'"))
                annotation_sql = ""

        return new_sql_list

    @classmethod
    def parse_sql_dynamicly(
        cls,
        sql: str,
        statement_id: str,
        worklist_type: int,
        worklist_id: int,
        schema_user: str,
        db_model: int,
        oracle_settings: dict,
        cmdb_id: int,
    ):
        # return: list(sql_plan) and dynamic_error = False | str(error_msg) and dynamic_error = True

        print(f"Params sql: {sql}, statement_id: {statement_id}, worklist_type: {worklist_type}, worklist_id: {worklist_id}, schema_user: {schema_user}, oracle_settings: {oracle_settings}")

        odb = OracleOB(**oracle_settings)
        try:

            if cls.is_explain_unvalid_sql(sql):
                return [], False

            # TODO commented it since our sql is passed through json, it has no annotations
            # sql = filter_annotation(sql)

            odb.execute(f"alter session set current_schema={schema_user}")
            odb.execute(f"EXPLAIN PLAN SET statement_id='{statement_id}' for {sql}")
            # 如果没有出错的话就往下跑 取出执行计划  get_sql_plan
            sql = f"SELECT * FROM plan_table WHERE statement_id = '{statement_id}'"
            sql_plans = odb.select_dict(sql, one=False)
            odb.execute(f"alter session set current_schema={oracle_settings['username']}")
            odb.close()
            if not sql_plans:
                raise Exception(f"No sqlplan for statement_id: {statement_id}, sql: {sql}")

            record_id = "#".join(["worklist", statement_id, schema_user])

            for sqlplan in sql_plans:
                insert_data = {
                    "sql_id": statement_id,  # statement_id
                    "schema": schema_user,
                    "etl_date": datetime.now(),
                    "ip_address": oracle_settings['host'],
                    "db_sid": oracle_settings['sid'],
                    "plan_hash_value": sqlplan['plan_id'],  # plan_id
                    "index": sqlplan['id'],  # id
                    "depth": sqlplan['depth'],  # depth
                    "parent_id": sqlplan['parent_id'],  # parent_id
                    "operation": sqlplan['operation'],  # operation
                    "operation_display": "",  # ??????
                    "options": sqlplan['options'],  # options
                    "object_node": sqlplan['object_node'],  # object_node
                    "object_owner": sqlplan['object_owner'],  # object_owner
                    "object_name": sqlplan['object_name'],  # objcet_name
                    "object_type": sqlplan['object_type'],  # object_type
                    "optimizer": sqlplan['optimizer'],  # optimizer
                    "search_columns": sqlplan['search_columns'],  # search_columns
                    "position": sqlplan['position'],  # position
                    "cost": sqlplan['cost'],  # cost
                    "cardinality": sqlplan['cardinality'],  # cardinality
                    "bytes": sqlplan['bytes'],  # bytes
                    "other_tag": sqlplan['other_tag'],  # other tag
                    "partition_start": sqlplan['partition_start'],  # partition_start
                    "partition_stop": sqlplan['partition_stop'],  # partition_stop
                    "partition_id": sqlplan['partition_id'],  # partition_id
                    "other": sqlplan['other'],  # other
                    "distribution": sqlplan['distribution'],  # distribution
                    "cpu_cost": sqlplan['cpu_cost'],  # cpu_cost
                    "io_cost": sqlplan['io_cost'],  # io_cost
                    "filter_predicates": sqlplan['filter_predicates'],  # filter_predicates
                    "access_predicates": sqlplan['access_predicates'],  # access_predicates
                    "time": sqlplan['time'],  # time
                    "cmdb_id": cmdb_id,
                    "record_id": record_id,
                }
                MongoHelper.insert('sqlplan', insert_data)

            keys = [x for x in sql_plans[0] if x not in ['other', 'other_xml']]
            values = [[worklist_id] + [sql_plan[x] for x in keys] for sql_plan in sql_plans]
            sql = "INSERT INTO T_SQL_PLAN(work_list_id, %s) VALUES(%s)" % (', '.join(keys), ', '.join([':%d' % x for x in range(1, len(keys) + 2)]))

            for sqlplan in values:
                OracleHelper.insert(sql, sqlplan)

            rule_descs = []
            for rule_name, params in RuleUtils.sqlplan(db_model).items():

                params['username'] = schema_user
                params['db_model'] = db_model
                params['ip_addr'] = oracle_settings['host']
                params['sid'] = oracle_settings['sid']
                if rule_name in worklist_type_dynamic_sqlplan_rule.get(worklist_type) and cls.sqlplan_parse(record_id, rule_name, params) is True:
                    rule_descs.append(params['rule_desc'])

            if rule_descs:
                raise Exception("\n".join(rule_descs))
            return sql_plans, False

        except Exception as e:
            return str(e), True

    @classmethod
    def sql_online(cls, sql, oracle_settings, schema_name):
        odb = OracleOB(**oracle_settings)
        try:
            odb.execute(f"alter session set current_schema={schema_name}")
            odb.execute(sql)
            odb.execute(f"alter session set current_schema={oracle_settings['username']}")
            odb.conn.commit()
            return ""
        except Exception as e:
            odb.conn.rollback()
            return str(e)
        finally:
            odb.conn.close()


if __name__ == "__main__":
    pass
