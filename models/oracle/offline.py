# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime
from typing import Union
from collections import defaultdict
from functools import reduce

from sqlalchemy import Column, String, Integer, Boolean, Sequence, Float
from sqlalchemy.dialects.oracle import DATE, CLOB

from .utils import BaseModel
from utils import const


class WorkList(BaseModel):
    __tablename__ = "T_WORK_LIST"

    work_list_id = Column("WORK_LIST_ID", Integer, Sequence('SEQ_WORK_LIST'), primary_key=True)
    # work_list_type = Column("WORK_LIST_TYPE", Integer)
    cmdb_id = Column("CMDB_ID", Integer)
    schema_name = Column("SCHEMA_NAME", String)
    task_name = Column("TASK_NAME", String)
    system_name = Column("SYSTEM_NAME", String)
    database_name = Column("DATABASE_NAME", String)
    sql_counts = Column("SQL_COUNTS", Integer)
    submit_date = Column("SUBMIT_DATE", DATE, default=datetime.now)
    submit_owner = Column("SUBMIT_OWNER", String, comment="发起人")
    audit_date = Column("AUDIT_DATE", DATE)  # 人工审核时间
    work_list_status = Column("WORK_LIST_STATUS", Integer,
                              default=const.OFFLINE_TICKET_ANALYSING)
    audit_role_id = Column("AUDIT_ROLE_ID", Integer)
    audit_owner = Column("AUDIT_OWNER", String, comment="审批人")
    audit_comments = Column("AUDIT_COMMENTS", String)
    online_date = Column("ONLINE_DATE", DATE)
    online_username = Column("ONLINE_USERNAME", String, comment="上线用户名")
    online_password = Column("ONLINE_PASSWORD", String, comment="上线密码")
    score = Column("SCORE", Float, comment="工单评分")
    db_type = Column("db_type", Integer, comment="数据库类型")

    def calc_score(self, at_least: Union[None, int, float] = 60):
        """
        根据已有的子工单诊断结果，计算当前工单的总分
        算分逻辑简述
        一个工单为一个整体，规则的最大扣分是共用的，也就是说不同子工单里规则最多扣分是算在一起的。
        每个子工单的每个规则的扣分加在一起，判断是否超过该规则的最大扣分，超过则去最大扣分，
        然后该工单下的所有扣分取和，除以所有启用的规则的最大扣分之和，乘以一百，
        最后看是否有遮羞分，如果有且小余遮羞分，则总分以遮羞分计，否则按实际计。
        :param session:
        :param at_least: 遮羞分，为None或者0的时候表示不需要
        :return:
        """
        from models.mongo import TicketRule
        from models.mongo import OracleTicketSubResult as TicketSubResult
        print("* calculating total score for offline ticket "
              f"with id: {self.work_list_id}...")
        # (3-key): (当前已扣, 最大扣分)
        rules_max_score = defaultdict(lambda: [0, 0])
        for rule in TicketRule.filter_enabled():
            rules_max_score[rule.unique_key()][1] = rule.max_score  # 赋值规则的最大扣分
        for sub_result in TicketSubResult.objects(work_list_id=self.work_list_id):
            static_and_dynamic_results = sub_result.static + sub_result.dynamic
            for item_of_sub_result in static_and_dynamic_results:
                rule_3_key = item_of_sub_result.get_rule_3_key()
                if rules_max_score[rule_3_key][0] < rules_max_score[rule_3_key][1]:
                    # 仅当已经扣掉的分数依然小于最大扣分的时候才继续扣分
                    rules_max_score[rule_3_key][0] -= item_of_sub_result.score_to_minus
                else:
                    # 否则，直接将扣分置为最大扣分
                    rules_max_score[rule_3_key][0] = rules_max_score[rule_3_key][1]
        total_minus_score, total_minus_score_max = reduce(
            lambda x, y: [x[0] + y[0], x[1] + y[1]],
            rules_max_score.values()
        )
        final_score = (total_minus_score_max - total_minus_score) / \
                      float(total_minus_score_max)
        if at_least and final_score < at_least:
            final_score = at_least
        self.score = final_score  # 未更新库中数据，需要手动加入session并commit


# TODO DEPRECATED!!!
# class SubWorkList(BaseModel):
#     __tablename__ = "T_SUB_WORK_LIST"
#
#     work_list_id = Column("WORK_LIST_ID", Integer)
#     statement_id = Column("STATEMENT_ID", String)
#     static_check_results = Column("STATIC_CHECK_RESULTS", CLOB)  # changed to CLOB
#     dynamic_check_results = Column("DYNAMIC_CHECK_RESULTS", CLOB)  # changed to CLOB
#     check_time = Column("CHECK_TIME", DATE, default=datetime.now)
#     check_owner = Column("CHECK_OWNER", String, comment="实际审批人")
#     check_status = Column("CHECK_STATUS", Boolean)
#     online_date = Column("ONLINE_DATE", DATE, default=datetime.now)
#     online_owner = Column("ONLINE_OWNER", String, comment="上线人")
#     elapsed_seconds = Column("ELAPSED_SECONDS", Integer)
#     status = Column("STATUS", Boolean)  # 上线是否成功
#     error_msg = Column("ERROR_MSG", String)
#     comments = Column("COMMENTS", String)
#     sql_text = Column("SQL_TEXT", CLOB)
#     id = Column("ID", Integer, Sequence("SEQ_T_SUB_WORK_LIST"), primary_key=True)


class WorkListAnalyseTemp(BaseModel):
    __tablename__ = "T_WORKLIST_ANALYSE_TEMP"

    id = Column("ID", Integer, Sequence("SEQ_T_WORKLIST_ANALYSE_TEMP"), primary_key=True)
    session_id = Column("SESSION_ID", String, nullable=False)
    sql_text = Column("SQL_TEXT", CLOB)
    comments = Column("COMMENTS", String)
    sql_type = Column("SQL_TYPE", Integer)
    analyse_date = Column("ANALYSE_DATE", DATE, default=datetime.now)
    num = Column("NUM", Integer)


class OSQLPlan(BaseModel):
    __tablename__ = "T_SQL_PLAN"

    work_list_id = Column("WORK_LIST_ID", Integer)
    statement_id = Column("STATEMENT_ID", String)
    plan_id = Column("PLAN_ID", Integer)
    timestamp = Column("TIMESTAMP", DATE, primary_key=True)
    remarks = Column("REMARKS", String)
    operation = Column("OPERATION", String)
    options = Column("OPTIONS", String)
    object_node = Column("OBJECT_NODE", String)
    object_owner = Column("OBJECT_OWNER", String)
    object_name = Column("OBJECT_NAME", String)
    object_alias = Column("OBJECT_ALIAS", String)
    object_instance = Column("OBJECT_INSTANCE", Integer)
    object_type = Column("OBJECT_TYPE", String)
    optimizer = Column("OPTIMIZER", String)
    search_columns = Column("SEARCH_COLUMNS", Integer)
    id = Column("ID", Integer)
    parent_id = Column("PARENT_ID", Integer)
    depth = Column("DEPTH", Integer)
    position = Column("POSITION", Integer)
    cost = Column("COST", Integer)
    cardinality = Column("CARDINALITY", Integer)
    bytes = Column("BYTES", Integer)
    other_tag = Column("OTHER_TAG", String)
    partition_start = Column("PARTITION_START", String)
    partition_stop = Column("PARTITION_STOP", String)
    partition_id = Column("PARTITION_ID", Integer)
    distribution = Column("DISTRIBUTION", String)
    cpu_cost = Column("CPU_COST", Integer)
    io_cost = Column("IO_COST", Integer)
    temp_space = Column("TEMP_SPACE", Integer)
    access_predicates = Column("ACCESS_PREDICATES", String)
    filter_predicates = Column("FILTER_PREDICATES", String)
    projection = Column("PROJECTION", String)
    time = Column("TIME", Integer)
    qblock_name = Column("QBLOCK_NAME", String)
