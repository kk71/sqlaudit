import uuid
import time
import random
import string

from utils.datetime_utils import *


class ReviewResult(object):

    def __init__(self, mongo_client, db_type, rule_type, username, rule_status, db_model="OLTP"):
        self.mongo_client = mongo_client
        self.db_type = db_type
        self.rule_type = rule_type
        self.rule_status = rule_status
        self.task_owner = username
        self.db_model = db_model
        self.task_id = None  # wait for job creation
        self.rule_info = self._get_rule_info()
        self.factor = [string.ascii_lowercase[:17], string.ascii_lowercase[17:]]

    # def gen_uuid(self):
    #     return str(uuid.uuid1())

    def _get_rule_info(self):
        """
        根据rule_type,db_type,rule_status等获取规则
        {
            "_id" : ObjectId("5ab23e9bd14e71299d85ed3b"),
            "db_type" : "O",
            "exclude_obj_type" : "",
            "input_parms" : [ { "parm_desc" : "or个数", "parm_name" : "or_num", "parm_value" : 5, "parm_unit" : "" } ],
            "max_score" : 20,
            "output_parms" : [ ],
            "rule_desc" : "多个过滤条件通过or连接",
            "rule_complexity" : "complex",
            "rule_cmd" : "default",
            "rule_name" : "TOOMANY_OR",
            "rule_status" : "ON",
            "rule_summary" : "多个过滤条件通过or连接,防止优化器出现选择异常",
            "rule_type" : "TEXT",
            "solution" : [ "改用临时表存入变量" ], "weight" : 2
        }
        """
        sql = {
            "rule_type": self.rule_type,
            "db_type": self.db_type,
            "rule_status": self.rule_status,
            "db_model": self.db_model
        }
        rule_data = self.mongo_client.find("rule", sql)
        temp = {}
        for value in rule_data:
            temp.update({
                value["rule_name"]: {
                    "weight": value["weight"],
                    "max_score": value["max_score"],
                    "input_parms": value["input_parms"],
                    "rule_desc": value["rule_desc"],
                    "rule_cmd": value["rule_cmd"],
                    "rule_complexity": value["rule_complexity"],
                    "rule_cmd_attach": value.get("rule_cmd_attach", None),
                    "obj_info_type": value.get("obj_info_type", None)
                }
            })
        return temp

    # def job_init(self, **kwargs):
    #     """
    #     初始化job信息，包括创建时间，创建用户，状态，任务id，以及一些描述信息等，返回任务id
    #     """
    #     task_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    #     capture_time_s = kwargs.get("startdate", "")
    #     capture_time_e = kwargs.get("stopdate", capture_time_s)
    #     operator_user = kwargs.get("operator_user")
    #     job_record = {
    #         "name": "#".join([self.task_owner, self.rule_type.lower()]),
    #         "cmdb_id": kwargs.get("cmdb_id"),
    #         "id": self.task_id,
            # "status": 2,
            # "create_time": arrow.get(task_start_time).datetime,
            # "etl_date": arrow.get(task_start_time).datetime,
            # "end_time": "",
            # "operator_user": operator_user,
            # "connect_name": kwargs.get("connect_name"),
            # "record_id": kwargs['record_id'],
            # "exported": False,
            # "desc": {
            #     "db_ip": kwargs.get("task_ip", "127.0.0.1"),
            #     "port": kwargs.get("task_port", 1521),
            #     "owner": self.task_owner,
            #     "rule_type": self.rule_type.upper(),
            #     "instance_name": kwargs.get("instance_name"),
            #     "capture_time_start": capture_time_s,
            #     "capture_time_end": capture_time_e
            # }
        # }
        # rst = self.mongo_client.insert_one("job", job_record)
        # self.task_id = str(rst.inserted_id)
        # return self.task_id

    def obj_result(self, results):
        for rule_name in results.keys():
            results[rule_name].update({
                "input_parms": self.rule_info[rule_name]["input_parms"],
                "rule_desc": self.rule_info[rule_name]["rule_desc"]
            })
        return results

    def oracle_result(self, sqlobj, sqltext, sqlplan, sqlstat):
        """
        生成oracle的规则解析结果
        """
        obj_temp = {}
        text_temp = {}
        stat_temp = {}
        obj_name_dict = {}
        cost_count = {}
        result = []
        for value in sqlobj:
            v1 = str(int(value[1]))
            if value[4] and value[7]:
                temp_key = "#".join([value[0], v1, str(int(value[4]))])
                value[7].update({"OBJECT_NAME": value[3]})
                obj_temp.update({temp_key: value[7]})
            if value[4]:
                temp_key = "#".join([value[0], v1, str(int(value[4]))])
                obj_name_dict.update({temp_key: value[3]})
                cost_count.update({temp_key: [value[5], value[6]]})
            else:
                temp_key = "#".join([value[0], v1, "v"])
                cost_count.update({temp_key: [value[5], value[6]]})
        # print(sqltext)
        for value in sqltext:
            text_temp.update({value[0]: value[1]})
        for value in sqlstat:
            v1 = str(int(value[1]))
            if self.rule_type == "SQLPLAN":
                if value[2]:
                    stat_id = "#".join([value[0], v1, str(int(value[2]))])
                else:
                    stat_id = "#".join([value[0], v1, "v"])
                stat_temp.update({stat_id: value[4]})
            elif self.rule_type == "SQLSTAT":
                stat_id = "#".join([value[0], v1, "v"])
                stat_temp.update({stat_id: value[3]})
        for value in sqlplan:
            v1 = str(int(value[1]))
            if self.rule_type == "SQLPLAN":
                if value[2]:
                    obj_id = "#".join([value[0], v1, str(int(value[2]))])
                    obj_name = obj_name_dict.get(obj_id, None)
                    result_obj = obj_temp.get(obj_id, {})
                else:
                    obj_id = "#".join([value[0], str(int(value[1])), "v"])
                    obj_name = None
                    result_obj = {}
                cost = cost_count.get(obj_id)[0]
                count = cost_count.get(obj_id)[1]
                # plan = value[3]
            elif self.rule_type == "SQLSTAT":
                obj_id = "#".join([value[0], str(int(value[1])), "v"])
                obj_name = None
                result_obj = {}
                # plan = value[2]
                cost = ""
                count = ""
            result_stat = stat_temp.get(obj_id, {})
            result_text = text_temp.get(value[0], {})
            result.append({
                    "sql_id": value[0],
                    "plan_hash_value": int(value[1]),
                    "schema": self.task_owner,
                    "sql_text": result_text.get("SQL_TEXT_DETAIL", None),
                    "stat": result_stat,
                    "obj_info": result_obj,
                    "obj_name": obj_name,
                    "cost": cost,
                    "count": count
            })
        return result

    # def get_obj(self, key, obj):
    #     pass

    # def mysql_result(self, sqlstat, sqltext, sqlplan, weight):
    #     """
    #     生成mysql的解析结果
    #     """
    #     results = {}
    #     for key, value in sqlstat.items():
    #         if value:
    #             results[key] = {}
    #             for data in value:
    #                 sql_id = "#".join([str(data["checksum"], "1", "v")])
    #                 temp_sql_paln = sqlplans[data["checksum"]]
    #                 temp_sql_text = sqltext[data["checksum"]]
    #                 if len(temp_sql_text) > 25:
    #                     text = temp_sql_text[:25]
    #                 else:
    #                     text = ""
    #                 rule_name = key
    #                 results[key].update({
    #                     sql_id: {
    #                         "sql_id": data["checksum"],
    #                         "plan_hash_value": int(1),
    #                         "sql_text": text,
    #                         "sql_fulltext": temp_sql_text,
    #                         "plan": temp_sql_plan,
    #                         "stat": data,
    #                         "obj_info": {},
    #                         "obj_name": None
    #                     }
    #                 })
    #             scores = len(value) * float(weight)
    #             results[key].update({
    #                 "input_parms": [],
    #                 "rule_name": key,
    #                 "rule_desc": desc,
    #                 "scores": scores
    #             })
    #     return results

    def gen_random_collection(self):
        """
        随机生成mongo中collection的名称
        """
        tmp0 = "tmp" + ''.join(random.sample(self.factor[0], 3))
        tmp1 = "tmp" + ''.join(random.sample(self.factor[1], 3))
        return tmp0, tmp1
