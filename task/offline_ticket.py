# Author: kk.Fang(fkfkbill@gmail.com)

import time
from datetime import datetime

from models import init_models
init_models()

import past.utils.utils
import past.utils.check
import past.models
from models.oracle import make_session, CMDB, WorkList, SubWorkList
from task.base import *
from utils.const import *


@celery.task
def offline_ticket(work_list_id, sqls):
    """
    把线下审核工单的每条语句拿过来，并且生成子工单
    :param work_list_id: 工单id
    :param sqls: [{"sql_text": str, "comments": str}, ...]
    """
    def get_old_cmdb(cmdb_id):
        old_cmdb = past.models.get_cmdb(
            cmdb_id=cmdb_id,
            select=["IP_ADDRESS as host", "port", "user_name as username", "password",
                    "service_name as sid", "db_model"]
        )
        old_cmdb.pop("db_model")
        return old_cmdb

    with make_session() as session:
        ticket = session.query(WorkList).filter_by(work_list_id=work_list_id).first()
        if not ticket:
            print(f"the ticket with id {work_list_id} is not found")
            return
        print(f"offline ticket {ticket.task_name} using schema {ticket.schema_name}"
              f" is going to be analyse...")
        cmdb = session.query(CMDB).filter_by(cmdb_id=ticket.cmdb_id).first()
        if not cmdb:
            print(f"the cmdb with id {ticket.cmdb_id} is not found")
            return

        # 目前的评分，总分写死100
        score = 100
        # 因为一个sql脚本是由多条sql语句组成的，
        # 每个sql语句都是需要跑完既定的所有规则，
        # 因此以扣分最多的那一条sql语句的得分作为静态分析的扣分
        # 动态分析也是同理
        static_minus_scores = []
        dynamic_minus_scores = []
        sql_types: set = {sql["sql_type"] for sql in sqls}

        for sql in sqls:
            sql_text: str = sql["sql_text"]
            comments: str = sql["comments"]
            sql_type: int = sql["sql_type"]

            start = time.time()
            # 静态分析
            static_error, static_score_to_minus = past.utils.check.Check.parse_single_sql(
                sql_text, sql_type, cmdb.db_model)

            # 特殊规则，判断是否ddl和dml混合在当前工单里了。
            # 如果有混合，则在每个sql子工单的静态审核结果里加上风险提示，
            if SQL_DML in sql_types and SQL_DDL in sql_types:
                static_error = "DML和DDL类型的语句混写 " + static_error

            static_minus_scores.append(static_score_to_minus)
            statement_id = past.utils.utils.get_random_str_without_duplicate()
            elapsed_second = int(time.time() - start)
            # 动态分析
            dynamic, dynamic_error, dynamic_score_to_minus = \
                past.utils.check.Check.parse_sql_dynamicly(
                    sql_text,
                    statement_id,
                    sql_type,
                    work_list_id,
                    ticket.schema_name,
                    cmdb.db_model,
                    get_old_cmdb(cmdb.cmdb_id),
                    cmdb.cmdb_id
                )
            if not dynamic_error and isinstance(dynamic, list):
                dynamic_check_results = ""
            elif isinstance(dynamic, str):
                dynamic_check_results = dynamic  # 有报错
            else:
                print(dynamic, type(dynamic))
            dynamic_minus_scores.append(dynamic_score_to_minus)

            # check_status 值的意义参阅数据库表定义的注释
            check_status = 1 if not dynamic_error and not static_error else 0  # 通过测试
            sub_ticket = SubWorkList(
                work_list_id=work_list_id,
                statement_id=statement_id,
                sql_text=sql_text,
                static_check_results=static_error,
                dynamic_check_results=dynamic_check_results,
                check_time=datetime.now(),
                check_status=check_status,
                elapsed_seconds=elapsed_second,
                # check_owner=ticket.audit_owner,
                comments=comments,
            )
            session.add(sub_ticket)

        ticket.sql_counts = len(sqls)

        # 取和
        sum_sms = sum(static_minus_scores)
        sum_dms = sum(dynamic_minus_scores)
        print(f"sum_sms = {sum_sms}, sum_dms = {sum_dms}")
        score = score + sum_sms + sum_dms

        # 特殊规则，判断是否ddl和dml混合在当前工单里了。
        # 如果有混合，则在每个sql子工单的静态审核结果里加上风险提示，
        # 但是整个工单只扣一次分数
        DDL_DML_MIXED_SCORE_TO_MINUS = 5
        if SQL_DML in sql_types and SQL_DDL in sql_types:
            score -= DDL_DML_MIXED_SCORE_TO_MINUS

        ticket.score = score if score > 60 else 60  # 给个分数下限显得好看一点
        session.add(ticket)
