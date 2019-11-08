import time
import signal
import traceback

from models import init_models
init_models()


import past.command
import plain_db.oracleob
import past.utils.utils
import past.utils.log
import past.utils.health_data_gen
import past.capture.sql

import utils.const
from task.base import *
import task.clear_cache
import utils.capture_utils
import utils.analyse_utils
from models.oracle import *
from models.mongo.utils import *
from utils.datetime_utils import *


logger = past.utils.log.get_logger("capture")


def sigintHandler(signum, frame):
    raise past.utils.utils.StopCeleryException("Stop!")


def update_record(task_id, record_id, success, err_msg=""):

    if success is True:
        # task execute successful  把where的task_id 换成record_id
        sql = """UPDATE t_task_exec_history
                 SET task_end_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'), status = 1
                 WHERE id = :2"""
        plain_db.oracleob.OracleHelper.update(sql, [past.utils.utils.get_time(return_str=True), record_id])
        sql = """UPDATE t_task_manage SET last_task_exec_succ_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'),
                 task_exec_success_counts = task_exec_success_counts + 1,
                 task_exec_counts = task_exec_counts + 1
                 WHERE task_id = :2
              """
        plain_db.oracleob.OracleHelper.update(sql, [past.utils.utils.get_time(return_str=True), task_id])
    else:

        status = 2 if success is None else 0
        # task execute failed  把where的task_id 换成record_id
        sql = """UPDATE t_task_exec_history
                 SET task_end_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'), status = :2, error_msg = :3
                 WHERE id = :3
              """
        plain_db.oracleob.OracleHelper.update(sql, [past.utils.utils.get_time(return_str=True), status, err_msg, record_id])

        sql = """UPDATE t_task_manage
                 SET task_exec_counts = task_exec_counts + 1
                 WHERE task_id = :1
              """
        plain_db.oracleob.OracleHelper.update(sql, [task_id])


def analysis_plan(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id):
    args = {
        "module": "analysis",
        "type": "SQLPLAN",
        "capture_date": now,
        "username": db_user,
        "create_user": "ISQLAUDIT",
        "db_type": utils.const.DB_ORACLE,
        "rule_type": "SQLPLAN",
        "rule_status": "ON",
        "cmdb_id": cmdb_id,
        "task_ip": host,
        "task_port": str(port),
        "sid": sid,
        "login_user": username,
        "password": password,
        "connect_name": connect_name,
        'record_id': record_id
    }
    com.run_analysis(**args)


def analysis_stat(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id):
    args = {
        "module": "analysis",
        "type": "SQLSTAT",
        "capture_date": now,
        "username": db_user,
        "create_user": "ISQLAUDIT",
        "sid": sid,
        "db_type": utils.const.DB_ORACLE,
        "rule_type": "SQLSTAT",
        "rule_status": "ON",
        "task_ip": host,
        "task_port": int(port),
        "cmdb_id": cmdb_id,
        "login_user": username,
        "password": password,
        "connect_name": connect_name,
        'record_id': record_id,
    }
    com.run_analysis(**args)


def analysis_text(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id):
    args = {
        "module": "analysis",
        "type": "TEXT",
        "username": db_user,
        "create_user": "ISQLAUDIT",
        "db_type": utils.const.DB_ORACLE,
        "sid": sid,
        "rule_type": "TEXT",
        "rule_status": "ON",
        "hostname": host,
        "task_ip": host,
        "task_port": str(port),
        "startdate": now,
        "stopdate": past.utils.utils.get_time(format="%Y-%m-%d"),  # 因为没用到
        "cmdb_id": cmdb_id,
        "login_user": username,
        "password": password,
        "connect_name": connect_name,
        'record_id': record_id
    }
    com.run_analysis(**args)


def analysis_obj(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, record_id):
    args = {
        "module": "analysis",
        "type": "OBJ",
        "db_server": host,
        "db_port": str(port),
        "create_user": "ISQLAUDIT",
        "username": db_user,
        "db_type": utils.const.DB_ORACLE,
        "rule_type": "OBJ",
        "rule_status": "ON",
        "task_ip": host,
        "task_port": str(port),
        "cmdb_id": cmdb_id,
        "login_user": username,
        "password": password,
        "sid": sid,
        "connect_name": connect_name,
        'record_id': record_id
    }
    com.run_analysis(**args)


def run_default_script(host, port, sid, username, password, db_user, cmdb_id, connect_name, record_id):

    com = past.command.Command()

    now = past.utils.utils.get_time()
    time.sleep(5)

    logger.info("run obj capture start...")
    com.run_capture(host, port, sid, username, password, past.utils.utils.get_time(return_str=True), "OBJ", connect_name, record_id, cmdb_id)
    logger.info("run other capture start...")
    com.run_capture(host, port, sid, username, password, past.utils.utils.get_time(format='%Y-%m-%d', return_str=True), "OTHER", connect_name, record_id, cmdb_id)

    print(">>>>>>>>>>>>>>>>>> now >>>>>>>>>>>>>>>>>")
    print(now)

    # plan
    logger.info("run analysis plan start...")
    analysis_plan(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id)

    # stat  这里的task_port 是数字类型的
    logger.info("run analysis stat start...")
    analysis_stat(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id)

    # text
    logger.info("run analysis text start...")
    analysis_text(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id)

    # obj
    logger.info("run analysis obj start...")
    analysis_obj(username, sid, host, port, com, db_user, cmdb_id, password, connect_name, record_id)


@celery.task
def task_run(host, port, sid, username, password,
             task_id, connect_name, business_name, db_users, cmdb_id, operator=None):
    """
    执行采集任务
    :param host:
    :param port:
    :param sid:
    :param username:
    :param password:
    :param task_id:
    :param connect_name:
    :param business_name:
    :param db_users:
    :param cmdb_id:
    :param operator: py表示app.py mkdata创建的任务，
                     None表示代码定时任务创建，其余记录操作的login_user
    :return:
    """

    # task_id -> task_manage.id, record_id -> t_task_exec_history.id
    signal.signal(signal.SIGTERM, sigintHandler)
    msg = f"{host}, {port}, {sid}, {username}, {password}, {task_id}," \
          f" {connect_name}, {business_name}, {db_users}, {operator}"
    logger.info(msg)

    from models.oracle import make_session, TaskExecHistory
    with make_session() as session:
        # 开始新任务之前，删除所有以前的pending任务，因为那些任务肯定已经挂了
        session.query(TaskExecHistory).filter(TaskExecHistory.status==None).delete()

    # 目前这个id已经废弃没有用了。
    task_uuid = celery.current_task.request.id

    with make_session() as session:
        # 写入任务
        task_record_object = TaskExecHistory(
            task_id=task_id,
            connect_name=connect_name,
            business_name=business_name,
            operator=operator
        )
        session.add(task_record_object)
        session.commit()
        session.refresh(task_record_object)
        record_id = task_record_object.id

    task.clear_cache.clear_cache.delay(no_prefetch=True)

    try:
        if not db_users:
            with make_session() as session:
                db_users: list = list(
                    session.query(RoleDataPrivilege.schema_name).
                    filter(RoleDataPrivilege.cmdb_id == cmdb_id)[0]
                )
        for user in db_users:
            run_default_script(host, port, sid, username, password, user, cmdb_id, connect_name, str(record_id) + "##" + user)
            logger.info("run script for health data...")
            past.utils.health_data_gen.calculate(record_id)
            utils.capture_utils.capture(record_id, cmdb_id, user, SchemaCapture)  # 新版采集per schema
        utils.capture_utils.capture(record_id, cmdb_id, None, CMDBCapture)  # 新版采集per CMDB
        utils.analyse_utils.calc_statistics(record_id, cmdb_id)  # 业务统计信息

        update_record(task_id, record_id, True)

    except past.utils.utils.StopCeleryException:
        logger.error(f"Stoping task: {task_id}!")
        update_record(task_id, record_id, None)

    except Exception as e:
        stack = traceback.format_exc()
        logger.error("Exception", exc_info=True)
        update_record(task_id, record_id, False, err_msg=stack)

    task.clear_cache.clear_cache.delay(no_prefetch=False)
    logger.warning("finish task ..........")
