""" 是一个定时脚本 每隔一分钟会去数据库里拉取数据
    如果执行任务的时间和当前时间相等
    则发布任务到Celery.

    读入配置文件 除了host和password和端口要变化以外 剩下的参数都是一样的
    执行四个脚本 capture_obj, capture
"""
import time
import traceback

import settings
from task.capture import task_run
from .mail_report import timing_send_mail
# from task.sqlaitune import sqlaitune_run
import plain_db.oracleob
from utils import const
from utils.datetime_utils import *


def get_time():
    return time.strftime("%R", time.localtime(time.time() + 60))


def time2int(timestamp: str, process_start_time: arrow.arrow):  # 返回目标的ts
    hour, minute = timestamp.split(":")
    return process_start_time.replace(hour=int(hour), minute=int(minute)).timestamp


def run_capture(now, process_start_time):
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
        task_begin_time_sec = time2int(task['schedule'], process_start_time)
        task_freq_sec = int(task['frequency']) * 60
        calced = (now.datetime.timestamp() - task_begin_time_sec) % task_freq_sec
        if calced < 1:
            print(f'task {task["task_id"]} is going to run for frequency '
                  f'is {task_freq_sec}s with calced={calced} ...')
            new_tasks.append(task)
    if new_tasks:
        print(f"Going to run {len(new_tasks)} tasks...")
    for task in new_tasks:
        print(task)

        if task['script'] == const.DB_TASK_CAPTURE:
            params = (task['host'], task['port'], task['sid'], task['user_name'], task['password'], task['task_id'],
                      task['connect_name'], task['business_name'], [], task['cmdb_id'], __file__)
            task_run.delay(*params)

        elif task['script'] == const.DB_TASK_TUNE:
            # sqlaitune_run.delay(task['task_id'], task['connect_name'], task['business_name'])
            pass

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
                }
            )

    if send_user_list:
        print("Mail ready to send: %s" % send_user_list)
        timing_send_mail.delay(send_user_list)


def main():
    process_start_time = arrow.now()
    print(f"Start schedule tasks at {dt_to_str(process_start_time)} ...")
    while True:
        try:
            now = arrow.now()
            next_to_run = now.shift(minutes=1).\
                replace(second=process_start_time.datetime.second)
            print(f"next time to run: {dt_to_str(next_to_run)}")
            time.sleep((next_to_run - now).seconds)

            # 每分钟执行一次
            run_capture(next_to_run, process_start_time)

            if next_to_run.minute == 0:  # 每小时执行一次
                run_mail(next_to_run)

        except:
            print(traceback.format_exc())


if __name__ == "__main__":
    main()

