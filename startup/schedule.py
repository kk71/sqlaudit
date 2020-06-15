# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

# initiate database models/connections

init_models()


def main():
    """start a task scheduler"""
    from task.scheduler import task_scheduler
    task_scheduler()
