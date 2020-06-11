# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "task_scheduler"
]

import time
import traceback

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
                # TODO
                print(a_task)

        except:
            print(traceback.format_exc())
