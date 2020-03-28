# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

# initiate database models/connections

init_models()

import task.schedule


def main():
    """start a task scheduler"""
    task.schedule.main()

