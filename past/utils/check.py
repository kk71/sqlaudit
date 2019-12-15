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
from utils.const import SQL_DDL as DDL
from .constant import worklist_type_static_rule
from .constant import worklist_type_dynamic_sqlplan_rule
from .constant import RULE_COMMANDS
from .utils import is_annotation, filter_annotation
from .constant import CREATE_APP_SQL, CREATE_DBA_SQL, OTHER_SQL, SQLPLUS_SQL

from models.mongo import Rule


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
    def text_parse(cls, key, rule_complexity, rule_cmd, input_params, sql, db_model):
        """处理静态审核的规则"""

        kwargs = {param["parm_name"]: param["parm_value"] for param in input_params}

        violate = False
        # 解析简单规则
        if rule_complexity == "simple" and re.search(rule_cmd, sql):
            violate = True
        elif rule_complexity == "complex":
            module_name = ".".join(["past.rule_analysis.rule.text", key.lower()])
            module = __import__(module_name, globals(), locals(), "execute_rule")
            ret = module.execute_rule(sql=sql, db_model=db_model, **kwargs)
            print(f"complex rule returned: {ret}")
            violate = ret is True
        return violate

    @classmethod
    def sqlplan_parse(cls, record_id, rule_name, params):
        """处理动态审核的规则"""

        print(f"* parsing dynamic rule {rule_name} with params: {params}")

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
    def parse_single_sql(cls, sql, single_sql_type, db_model) -> tuple:
        """
        单条SQL语句的静态分析，得出该语句的扣分以及问题信息
        :param sql: 单条语句文本
        :param single_sql_type: 当前语句的类型是ddl还是dml
        :param db_model:
        :return: (错误信息, 扣分负数)
        """
        formatted_sql = sqlparse.format(sql, strip_whitespace=True).lower()
        minus_score = 0  # 负数！
        err_msgs = []

        rule_q = Rule.filter_enabled(db_model=db_model)

        for rule in rule_q:
            if rule.rule_name in worklist_type_static_rule[single_sql_type]:
                try:
                    print(f"parsing static rule {rule.rule_name}...")
                    err = cls.text_parse(
                        rule.rule_name,
                        rule.rule_complexity,
                        rule.rule_cmd,
                        rule.input_parms,
                        formatted_sql,
                        db_model
                    )
                    if err:
                        minus_score -= rule.weight  # weight才是真正的单次扣分
                        err_msgs.append(rule.rule_desc)
                except Exception as err:
                    err_msgs.append(rule.rule_desc)
                    minus_score -= rule.weight  # weight才是真正的单次扣分
                    logging.error("Exception:", exc_info=True)
        return "" if not err_msgs else '\n'.join(err_msgs), minus_score

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
        sql_type: int,
        worklist_id: int,
        schema_user: str,
        db_model: int,
        oracle_settings: dict,
        cmdb_id: int,
    ):
        """

        :param sql:
        :param statement_id:
        :param sql_type:
        :param worklist_id:
        :param schema_user:
        :param db_model:
        :param oracle_settings:
        :param cmdb_id:
        :return: ()
        """
        # return: list(sql_plan) and dynamic_error = False | str(error_msg) and dynamic_error = True

        print(f"Params sql: {sql}, statement_id: {statement_id}, "
              f"sql_type: {sql_type}, worklist_id: {worklist_id},"
              f" schema_user: {schema_user}, oracle_settings: {oracle_settings}")

        odb = OracleOB(**oracle_settings)
        minus_scores = 0
        try:
        # if True:

            if cls.is_explain_unvalid_sql(sql):
                return [], False, 0  # 无报错

            sql = filter_annotation(sql)

            if schema_user:
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
                    "SQL_ID": statement_id,  # statement_id
                    "USERNAME": schema_user,
                    "ETL_DATE": datetime.now(),
                    "IPADDR": oracle_settings['host'],
                    "DB_SID": oracle_settings['sid'],
                    "PLAN_HASH_VALUE": sqlplan['plan_id'],  # plan_id
                    "ID": sqlplan['id'],  # id
                    "DEPTH": sqlplan['depth'],  # depth
                    "PARENT_ID": sqlplan['parent_id'],  # parent_id
                    "OPERATION": sqlplan['operation'],  # operation
                    "OPERATION_DISPLAY": " " * sqlplan["depth"] + sqlplan["operation"],
                    "OPTIONS": sqlplan['options'],  # options
                    "OBJECT_NODE": sqlplan['object_node'],  # object_node
                    "OBJECT_OWNER": sqlplan['object_owner'],  # object_owner
                    "OBJECT_NAME": sqlplan['object_name'],  # objcet_name
                    # object_alias
                    # object_instance
                    # time_space
                    # projection
                    # qblock name
                    "OBJECT_TYPE": sqlplan['object_type'],  # object_type
                    "OPTIMIZER": sqlplan['optimizer'],  # optimizer
                    "SEARCH_COLUMNS": sqlplan['search_columns'],  # search_columns
                    "POSITION": sqlplan['position'],  # position
                    "COST": sqlplan['cost'],  # cost
                    "CARDINALITY": sqlplan['cardinality'],  # cardinality
                    "BYTES": sqlplan['bytes'],  # bytes
                    "OTHER_TAG": sqlplan['other_tag'],  # other tag
                    "PARTITION_START": sqlplan['partition_start'],  # partition_start
                    "PARTITION_STOP": sqlplan['partition_stop'],  # partition_stop
                    "PARTITION_ID": sqlplan['partition_id'],  # partition_id
                    "OTHER": sqlplan['other'],  # other
                    "DISTRIBUTION": sqlplan['distribution'],  # distribution
                    "CPU_COST": sqlplan['cpu_cost'],  # cpu_cost
                    "IO_COST": sqlplan['io_cost'],  # io_cost
                    "FILTER_PREDICATES": sqlplan['filter_predicates'],  # filter_predicates
                    "ACCESS_PREDICATES": sqlplan['access_predicates'],  # access_predicates
                    "TIME": sqlplan['time'],  # time
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
                if rule_name in worklist_type_dynamic_sqlplan_rule.get(sql_type) and\
                        cls.sqlplan_parse(record_id, rule_name, params) is True:
                    rule_descs.append(params['rule_desc'])
                    minus_scores -= params["weight"]

            if rule_descs:
                raise Exception("\n".join(rule_descs))
            return sql_plans, False, minus_scores

        except Exception as e:
            return str(e), True, -100

    @classmethod
    def sql_online(cls, sql, oracle_settings, schema_name):
        odb = OracleOB(**oracle_settings)
        try:
            # odb.execute(f"alter session set current_schema={schema_name}")
            odb.execute(sql)
            # odb.execute(f"alter session set current_schema={oracle_settings['username']}")
            odb.conn.commit()
            return ""
        except Exception as e:
            odb.conn.rollback()
            return str(e)
        finally:
            odb.conn.close()


if __name__ == "__main__":
    pass
