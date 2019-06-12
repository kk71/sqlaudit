import sys

import settings

import past.capture.sql
import task.capture as task_capture
import plain_db.oracleob


def run(task_id, schema=None, use_queue=False):
    odb = plain_db.oracleob.OracleOB(settings.ORACLE_IP, settings.ORACLE_PORT, settings.ORACLE_USERNAME, settings.ORACLE_PASSWORD, settings.ORACLE_SID)
    sql = f"""SELECT tm.task_id, tm.connect_name, tm.group_name, tm.business_name, tm.machine_room,
                    tm.database_type, tm.ip_address AS host, tm.port AS port,
                    tm.task_schedule_date AS schedule, tm.task_exec_scripts AS script, c.service_name AS sid,
                    c.user_name, c.password, tm.task_exec_frequency AS frequency, c.cmdb_id AS cmdb_id
             FROM T_TASK_MANAGE tm INNER JOIN T_CMDB c ON c.cmdb_id = tm.cmdb_id
             WHERE tm.task_status = 1 AND c.status = 1 AND c.is_collect = 1 AND task_id = {task_id}
          """
    task = odb.select_dict(sql, one=True)
    if not task:
        print("No such task.... Please enter correct task id")
        exit()

    cmdb_odb = plain_db.oracleob.OracleOB(task['host'], task['port'], task['user_name'], task['password'], task['sid'])
    if task['script'] == "采集及分析":
        users = [x[0] for x in cmdb_odb.select(past.capture.sql.GET_SCHEMA, one=False)]
        if schema and schema in users:
            users = [schema]
        print(users)
        if use_queue:
            task_uuid = task_capture.task_run.delay(task['host'], task['port'], task['sid'], task['user_name'], task['password'], task['task_id'], task['connect_name'], task['business_name'], users, task['cmdb_id'])
        else:
            task_uuid = task_capture.task_run(task['host'], task['port'], task['sid'], task['user_name'], task['password'], task['task_id'], task['connect_name'], task['business_name'], users, task['cmdb_id'])
        print(task_uuid)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("please enter the task_id")
        exit()
    if len(sys.argv) == 2:
        run(sys.argv[1])
    elif len(sys.argv) == 3:
        run(sys.argv[1], sys.argv[2])

