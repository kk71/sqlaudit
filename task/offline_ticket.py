# Author: kk.Fang(fkfkbill@gmail.com)

import time
import settings
from datetime import datetime

from celery import Celery, platforms
from utils.sql_utils import SQL_DDL, SQL_DML

from models import init_models

platforms.C_FORCE_ROOT = True
celery = Celery("offline_ticket",
                backend=settings.REDIS_BROKER,
                broker=settings.REDIS_BROKER,
                )
celery.conf.update(settings.CELERY_CONF)

init_models()


import past.utils.utils
import past.utils.check
import past.models
from models.oracle import make_session, CMDB, WorkList, SubWorkList


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
            select=["IP_ADDRESS as host", "port", "user_name as username", "password", "service_name as sid", "db_model"]
        )
        old_cmdb.pop("db_model")
        return old_cmdb

    with make_session() as session:
        ticket = session.query(WorkList).filter_by(work_list_id=work_list_id).first()
        if not ticket:
            print(f"the ticket with id {work_list_id} is not found")
            return
        cmdb = session.query(CMDB).filter_by(cmdb_id=ticket.cmdb_id).first()
        if not cmdb:
            print(f"the cmdb with id {ticket.cmdb_id} is not found")
            return

        for sql in sqls:
            sql_text = sql["sql_text"]
            comments = sql["comments"]

            work_list_type = SQL_DDL if 'create' in sql_text or \
                                        'drop' in sql_text or \
                                        'alter' in sql_text \
                else SQL_DML
            start = time.time()
            static_error = past.utils.check.Check.parse_single_sql(
                sql_text, work_list_type, cmdb.db_model)
            statement_id = past.utils.utils.get_random_str_without_duplicate()
            elapsed_second = int(time.time() - start)
            # TODO 时间问题，暂时用恶心写法。
            dynamic, dynamic_error = past.utils.check.Check.parse_sql_dynamicly(
                sql_text,
                statement_id,
                work_list_type,
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
                check_owner=ticket.submit_owner,
                comments=comments
            )
            session.add(sub_ticket)
