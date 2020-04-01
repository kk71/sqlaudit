import signal
import traceback

from models import init_models

init_models()

import past.command
import plain_db.oracleob
import past.utils.utils
import past.utils.log
import past.capture.sql

import utils.const
from task.base import *
import task.clear_cache
import utils.capture_utils
import utils.analyse_utils
import utils.cmdb_utils
from models.mongo.utils import *

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
        plain_db.oracleob.OracleHelper.update(sql,
                                              [past.utils.utils.get_time(return_str=True), status, err_msg, record_id])

        sql = """UPDATE t_task_manage
                 SET task_exec_counts = task_exec_counts + 1
                 WHERE task_id = :1
              """
        plain_db.oracleob.OracleHelper.update(sql, [task_id])


def analysis_plan(
        username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id):
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


def analysis_stat(
        username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id):
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


def analysis_text(
        username, sid, host, port, com, db_user, cmdb_id, password, connect_name, now, record_id):
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


def analysis_obj(
        username, sid, host, port, com, db_user, cmdb_id, password, connect_name, record_id):
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


def run_old_capture(
        host, port, sid, username, password, cmdb_id, connect_name, record_id):
    com = past.command.Command()

    logger.info("old object capture start...")
    com.run_capture(host, port, sid, username, password,
                    past.utils.utils.get_time(return_str=True),
                    "OBJ", connect_name, record_id, cmdb_id)
    logger.info("old other capture start...")
    com.run_capture(host, port, sid, username, password,
                    past.utils.utils.get_time(format='%Y-%m-%d', return_str=True),
                    "OTHER", connect_name, record_id, cmdb_id)


def analyse_rule_by_schema(
        host, port, sid, username, password, db_user, cmdb_id, connect_name, record_id):
    com = past.command.Command()
    now = past.utils.utils.get_time()

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
def task_run(task_id, db_users, cmdb_id, operator=None):
    """
    执行采集任务
    :param task_id:
    :param db_users:
    :param cmdb_id:
    :param operator: py表示app.py mkdata创建的任务，
                     None表示代码定时任务创建，其余记录操作的login_user
    :return:
    """
    from models.oracle import make_session, TaskExecHistory, CMDB
    with make_session() as session:
        cmdb = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).first()
        host = cmdb.ip_address
        port = cmdb.port
        sid = cmdb.service_name
        username = cmdb.user_name
        password = cmdb.password
        connect_name = cmdb.connect_name
        business_name = cmdb.business_name

    # task_id -> task_manage.id, record_id -> t_task_exec_history.id
    signal.signal(signal.SIGTERM, sigintHandler)
    msg = f"{host}, {port}, {sid}, {task_id}, {connect_name}, " \
          f"{business_name}, {db_users}, {operator}"
    logger.info(msg)

    print(f"* start cleaning unavailable schemas in current cmdb({cmdb_id})...")
    from utils.cmdb_utils import clean_unavailable_schema
    with make_session() as session:
        clean_unavailable_schema(session, cmdb_id)
    print("done.\n\n")

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
        task_record_id = task_record_object.id
        print(f"* current task task_record_id: {task_record_id}")

    task.clear_cache.clear_cache.delay(no_prefetch=True)

    try:
        if not db_users:
            with make_session() as session:
                db_users: list = utils.cmdb_utils.get_cmdb_bound_schema(session, cmdb_id)
                if not db_users:
                    raise utils.const.CMDBHasNoSchemaBound
                print(f"going to capture the following schema(s): {db_users}")
        run_old_capture(host, port, sid, username, password, cmdb_id,
                        connect_name, task_record_id)
        for user in db_users:
            utils.capture_utils.capture(
                task_record_id, cmdb_id, user, SchemaCapture)  # 新版采集per schema
            analyse_rule_by_schema(
                host, port, sid, username, password, user, cmdb_id,
                connect_name, str(task_record_id) + "##" + user)
            # past.utils.health_data_gen.calculate(record_id)
        utils.capture_utils.capture(
            task_record_id, cmdb_id, None, CMDBCapture)  # 新版采集per CMDB
        utils.analyse_utils.calc_statistics(
            task_record_id, cmdb_id)  # 业务统计信息

        update_record(task_id, task_record_id, True)

    except past.utils.utils.StopCeleryException:
        logger.error(f"Stoping task: {task_id}!")
        update_record(task_id, task_record_id, None)

    except Exception as e:
        stack = traceback.format_exc()
        logger.error("Exception", exc_info=True)
        update_record(task_id, task_record_id, False, err_msg=stack)

    task.clear_cache.clear_cache.delay(no_prefetch=False)
    logger.warning(f"Task finished(task_record_id: {task_record_id})..........")