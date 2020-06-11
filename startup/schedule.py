# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

# initiate database models/connections

init_models()

from task.scheduler import task_scheduler


def main():
    """start a task scheduler"""
    task_scheduler()
