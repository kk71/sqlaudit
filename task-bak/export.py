# Author: kk.Fang(fkfkbill@gmail.com)

from html_report.export import export_task
from .base import celery


@celery.task
def export(task_id):
    export_task(task_id)
