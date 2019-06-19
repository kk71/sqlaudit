# Author: kk.Fang(fkfkbill@gmail.com)

import settings
import past.utils.log
from past.utils.utils import get_time
from plain_db.oracleob import OracleOB
from task.base import *

logger = past.utils.log.get_logger("sqlaitune")


@celery.task
def sqlaitune_run(task_id, connect_name, business_name):
    logger.info("sqlaitune_run")

    odb = OracleOB(settings.ORACLE_IP,
                   settings.ORACLE_PORT,
                   settings.ORACLE_USERNAME,
                   settings.ORACLE_PASSWORD,
                   settings.ORACLE_SID)
    sql = """INSERT INTO t_task_exec_history(id, task_id, connect_name, business_name, task_start_date)
             VALUES(SEQ_TASK_EXEC_HISTORY.nextval, :1, :2, :3, to_date(:4, 'yyyy-mm-dd hh24:mi:ss'))
          """
    odb.insert(sql, [task_id, connect_name, business_name, get_time(return_str=True)])
    sql = "SELECT SEQ_TASK_EXEC_HISTORY.CURRVAL FROM DUAL"
    record_id = odb.select(sql, one=True)[0]

    try:
        # for user in db_users:
        # run_sqlaitune()

        # task execute successful  把where的task_id 换成record_id
        sql = """UPDATE t_task_exec_history
                 SET task_end_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'), status = 1
                 WHERE id = :2"""

        odb.update(sql, [get_time(return_str=True), record_id])
        sql = """UPDATE t_task_manage SET last_task_exec_succ_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'),
                 task_exec_success_counts = task_exec_success_counts + 1,
                 task_exec_counts = task_exec_counts + 1
                 WHERE task_id = :2
              """
        odb.update(sql, [get_time(return_str=True), task_id])

    except Exception as e:
        logger.error("Exception", exc_info=True)

        # task execute failed  把where的task_id 换成record_id
        sql = """UPDATE t_task_exec_history
                 SET task_end_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'), status = 0, error_msg = :2
                 WHERE id = :3
              """
        odb.update(sql, [get_time(return_str=True), str(e), record_id])

        sql = """UPDATE t_task_manage
                 SET task_exec_counts = task_exec_counts + 1
                 WHERE task_id = :1
              """
        odb.update(sql, [task_id])

    logger.warning("finish task ..........")
