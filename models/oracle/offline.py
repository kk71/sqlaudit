# Author: kk.Fang(fkfkbill@gmail.com)

from datetime import datetime
from typing import Union
from collections import defaultdict
from functools import reduce

from sqlalchemy import Column, String, Integer, Sequence, Float
from sqlalchemy.dialects.oracle import DATE, CLOB

from .utils import BaseModel
from utils import const


class WorkList(BaseModel):
    __tablename__ = "T_WORK_LIST"

    work_list_id = Column("WORK_LIST_ID", Integer, Sequence('SEQ_WORK_LIST'),
                          primary_key=True)
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
    db_type = Column("DB_TYPE", String, comment="数据库类型")

    def __str__(self):
        return f"ticket: {self.db_type}-{self.work_list_id}"

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
        from new_rule.rule import TicketRule
        from models.mongo import OracleTicketSubResult as _TicketSubResult
        print("* calculating total score for offline ticket "
              f"with id: {self.work_list_id}...")
        # unique_key: (当前已扣, 最大扣分)
        rules_max_score = defaultdict(lambda: [0, 0])
        for rule in TicketRule.filter_enabled():
            rules_max_score[rule.unique_key()][1] = rule.max_score  # 赋值规则的最大扣分
        if not rules_max_score:
            print("no enabled ticket rules.")
            return  # 没有可用的规则。
        for sub_result in _TicketSubResult.objects(work_list_id=self.work_list_id):
            static_and_dynamic_results = sub_result.static + sub_result.dynamic
            for item_of_sub_result in static_and_dynamic_results:
                rule_unique_key = item_of_sub_result.get_rule_unique_key()
                if rules_max_score[rule_unique_key][0] < \
                        rules_max_score[rule_unique_key][1]:
                    # 仅当已经扣掉的分数依然小于最大扣分的时候才继续扣分
                    rules_max_score[rule_unique_key][0] += \
                        item_of_sub_result.minus_score  # 这个minus_score是负数或0！
                else:
                    # 否则，直接将扣分置为最大扣分
                    rules_max_score[rule_unique_key][0] = \
                        rules_max_score[rule_unique_key][1]
        total_minus_score, _ = reduce(
            lambda x, y: [x[0] + y[0], x[1] + y[1]],
            rules_max_score.values()
        )
        all_rule_max_score_sum = TicketRule.calc_score_max_sum(db_type=self.db_type)
        if all_rule_max_score_sum:
            final_score = (all_rule_max_score_sum + total_minus_score) / \
                        float(all_rule_max_score_sum) * 100.0
        else:
            final_score = 100
        if at_least and final_score < at_least:
            final_score = at_least
        self.score = round(final_score, 2)  # 未更新库中数据，需要手动加入session并commit


class WorkListAnalyseTemp(BaseModel):
    __tablename__ = "T_WORKLIST_ANALYSE_TEMP"

    id = Column("ID", Integer, Sequence("SEQ_T_WORKLIST_ANALYSE_TEMP"),
                primary_key=True)
    session_id = Column("SESSION_ID", String, nullable=False)
    sql_text = Column("SQL_TEXT", CLOB)
    sql_text_no_comment = Column("SQL_TEXT_NO_COMMENT", CLOB)
    comments = Column("COMMENTS", String)
    sql_type = Column("SQL_TYPE", Integer)
    analyse_date = Column("ANALYSE_DATE", DATE, default=datetime.now)
    num = Column("NUM", Integer)
