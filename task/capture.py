import time
import signal

from celery import Celery, platforms

import settings

import past.command
import plain_db.oracleob
import past.utils.utils
import past.utils.log
import past.utils.health_data_gen


platforms.C_FORCE_ROOT = True
celery = Celery("task_capture",
                backend=settings.REDIS_BACKEND,
                broker=settings.REDIS_BROKER,
                include=["task_capture", "task_exports"]
                )
logger = past.utils.log.get_logger("capture")
celery.conf.update(settings.CELERY_CONF)


def sigintHandler(signum, frame):
    raise past.utils.utils.StopCeleryException("Stop!")


def init_job(task_id, connect_name, business_name, task_uuid):

    sql = """INSERT INTO t_task_exec_history(id, task_id, connect_name, business_name, task_start_date, task_uuid)
             VALUES(SEQ_TASK_EXEC_HISTORY.nextval, :1, :2, :3, to_date(:4, 'yyyy-mm-dd hh24:mi:ss'), :5)
          """
    plain_db.oracleob.OracleHelper.insert(sql, [task_id, connect_name, business_name, past.utils.utils.get_time(), task_uuid])
    sql = "SELECT SEQ_TASK_EXEC_HISTORY.CURRVAL FROM DUAL"
    record_id = plain_db.oracleob.OracleHelper.select(sql, one=True)[0]
    return record_id


def update_record(task_id, record_id, success, err_msg=""):

    if success is True:
        # task execute successful  把where的task_id 换成record_id
        sql = """UPDATE t_task_exec_history
                 SET task_end_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'), status = 1
                 WHERE id = :2"""
        plain_db.oracleob.OracleHelper.update(sql, [past.utils.utils.get_time(), record_id])
        sql = """UPDATE t_task_manage SET last_task_exec_succ_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'),
                 task_exec_success_counts = task_exec_success_counts + 1,
                 task_exec_counts = task_exec_counts + 1
                 WHERE task_id = :2
              """
        plain_db.oracleob.OracleHelper.update(sql, [past.utils.utils.get_time(), task_id])
    else:

        status = 2 if success is None else 0
        # task execute failed  把where的task_id 换成record_id
        sql = """UPDATE t_task_exec_history
                 SET task_end_date = to_date(:1, 'yyyy-mm-dd hh24:mi:ss'), status = :2, error_msg = :3
                 WHERE id = :3
              """
        plain_db.oracleob.OracleHelper.update(sql, [past.utils.utils.get_time(), status, err_msg, record_id])

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
        "db_type": "O",
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
        "db_type": "O",
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
        "db_type": "O",
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
        "db_type": "O",
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
    com.run_capture(host, port, sid, username, password, past.utils.utils.get_time(), "OBJ", connect_name, record_id, cmdb_id)
    logger.info("run other capture start...")
    com.run_capture(host, port, sid, username, password, past.utils.utils.get_time(format='%Y-%m-%d'), "OTHER", connect_name, record_id, cmdb_id)

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
def task_run(host, port, sid, username, password, task_id, connect_name, business_name, db_users, cmdb_id):

    # task_id -> task_manage.id, record_id -> t_task_exec_history.id
    signal.signal(signal.SIGTERM, sigintHandler)
    msg = f"{host}, {port}, {sid}, {username}, {password}, {task_id}, {connect_name}, {business_name}, {db_users}"
    logger.info(msg)

    task_uuid = celery.current_task.request.id
    record_id = init_job(task_id, connect_name, business_name, task_uuid)

    try:
        for user in db_users:
            run_default_script(host, port, sid, username, password, user, cmdb_id, connect_name, str(record_id) + "##" + user)
            logger.info("run script for health data...")
            past.utils.health_data_gen.calculate()

        update_record(task_id, record_id, True)

    except past.utils.utils.StopCeleryException:
        logger.error(f"Stoping task: {task_id}!")
        update_record(task_id, record_id, None)

    except Exception as e:
        logger.error("Exception", exc_info=True)
        update_record(task_id, record_id, False, err_msg=str(e))

    logger.warning("finish task ..........")
