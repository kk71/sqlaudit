# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "task_scheduler"
]

import time
import traceback

import task.celery_collect  # 不要删掉，导入即是扫描并收集所有任务!
from utils.datetime_utils import *
from .task import all_task_instances


def task_scheduler():
    """定时任务的调度进程入口"""
    process_start_time = arrow.now()
    print(f"* Task scheduler started at {dt_to_str(process_start_time)} ...")
    while True:
        try:
            now = arrow.now()
            next_to_run = now.shift(minutes=1).\
                replace(second=process_start_time.datetime.second)
            print(f"next time to run: {dt_to_str(next_to_run)}")
            time.sleep((next_to_run - now).seconds)

            for a_task in all_task_instances:
                the_scheduler = getattr(a_task, "schedule", None)
                if the_scheduler and callable(the_scheduler):
                    try:
                        print(f"{a_task}... ", end="")
                        the_scheduler(now, process_start_time)
                        print("done.")
                    except NotImplementedError:
                        print("not scheduled.")
        except:
            print(traceback.format_exc())
