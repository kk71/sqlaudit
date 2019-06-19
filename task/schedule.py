""" 是一个定时脚本 每隔一分钟会去数据库里拉取数据
    如果执行任务的时间和当前时间相等
    则发布任务到Celery.

    读入配置文件 除了host和password和端口要变化以外 剩下的参数都是一样的
    执行四个脚本 capture_obj, capture
"""
import time
import traceback
from datetime import datetime

import cx_Oracle

import settings
from task.capture import task_run
import past.capture.sql
# from task_mail import timing_send_email
from task.sqlaitune import sqlaitune_run
import plain_db.oracleob
from utils import const
# from task_mongo import clean_mongo


def get_time():
    return time.strftime("%R", time.localtime(time.time() + 60))


def get_sleep_time():
    return 60 - int(time.time()) % 60


def time2int(timestamp: str):  # 返回目标的ts
    minute, second = timestamp.split(":")
    return int(minute) * 3600 + int(second) * 60


def run_capture(now):
    odb = plain_db.oracleob.OracleOB(settings.ORACLE_IP, settings.ORACLE_PORT,
                                     settings.ORACLE_USERNAME, settings.ORACLE_PASSWORD,
                                     settings.ORACLE_SID)

    sql = """SELECT tm.task_id, tm.connect_name, tm.group_name, tm.business_name, tm.machine_room,
                    tm.database_type, tm.ip_address AS host, tm.port AS port,
                    tm.task_schedule_date AS schedule, tm.task_exec_scripts AS script, c.service_name AS sid,
                    c.user_name, c.password, tm.task_exec_frequency AS frequency, c.cmdb_id AS cmdb_id
             FROM T_TASK_MANAGE tm INNER JOIN T_CMDB c ON c.cmdb_id = tm.cmdb_id
             WHERE tm.task_status = 1 AND c.status = 1 AND c.is_collect = 1
          """
    tasks = odb.select_dict(sql, one=False)
    new_tasks = []
    for task in tasks:
        if not task["schedule"] or not task["frequency"]:
            print(f"task with id {task['id']} has neither schedule date nor frequency.")
            continue
        if (now - time2int(task['schedule'])) % (int(task['frequency']) * 60) == 0:
            new_tasks.append(task)
    if new_tasks:
        print(f"Going to run {len(new_tasks)} tasks...")
    for task in new_tasks:
        print(task)
        try:
            cmdb_odb = plain_db.oracleob.OracleOB(task['host'], task['port'], task['user_name'], task['password'], task['sid'])
        except cx_Oracle.DatabaseError as err:
            print(str(err))
            continue

        if task['script'] == const.DB_TASK_CAPTURE:
            sql = past.capture.sql.GET_SCHEMA
            users = [x[0] for x in cmdb_odb.select(sql, one=False)]

            params = [task['host'], task['port'], task['sid'], task['user_name'], task['password'], task['task_id'],
                      task['connect_name'], task['business_name'], users, task['cmdb_id']]
            task_run.delay(*params)

        elif task['script'] == const.DB_TASK_TUNE:
            sqlaitune_run.delay(task['task_id'], task['connect_name'], task['business_name'])

        else:
            assert 0


def run_mail(time_structure):

    weekday = time_structure.weekday()
    hour = time_structure.hour

    sql = "SELECT * FROM MAIL_SERVER"
    server_data = plain_db.oracleob.OracleHelper.select_dict(sql, one=True)
    if not server_data or int(server_data['status']) == 0:
        return

    # 获得当日此时应该发送的邮件列表
    sql = "SELECT * FROM SEND_MAIL_LIST"
    mail_data = plain_db.oracleob.OracleHelper.select_dict(sql, one=False)

    # 获得要发送的用户有哪些
    send_user_list = list()
    for mail_detail in mail_data:
        send_date = mail_detail['send_date']
        send_date = send_date.split('-')
        week_day = send_date[0]
        time_point = send_date[1]

        # 获得应该发送的用户信息
        if int(week_day) == weekday and int(time_point) == hour:
            send_user = mail_detail['mail_sender']
            send_user = send_user.split(';')
            send_user_list.append(
                {
                    "send_mail_id": mail_detail['send_mail_id'],
                    "title": mail_detail['title'],
                    "contents": mail_detail['contents'],
                    "mail_sender": send_user,
                    # "mail_list": [],
                }
            )

    if send_user_list:
        print("Mail ready to send: %s" % send_user_list)
        # timing_send_email.delay(send_user_list)


def main():
    print("Start schedule tasks ...")
    while True:
        try:

            now = time.time()
            next_minute_ts = now + 60 - now % 60
            next_minute_structure = datetime.fromtimestamp(next_minute_ts)
            t = get_sleep_time()

            if next_minute_structure.minute % 5 == 0:
                print(next_minute_structure.strftime("%Y-%m-%d %X"))

            time.sleep(t)
            run_capture(next_minute_ts + 3600 * 8)

            # if next_minute_structure.minute == 0:  # 每小时执行一次
            #     run_mail(next_minute_structure)

            # if next_minute_structure.minute == 0 & next_minute_structure.hour == 0 & next_minute_structure.day == 1:
            #     clean_mongo.delay()

        except:
            print(traceback.format_exc())


if __name__ == "__main__":
    main()

