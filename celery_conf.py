# Author: kk.Fang(fkfkbill@gmail.com)

from celery import platforms
from kombu import Exchange, Queue

import settings


platforms.C_FORCE_ROOT = True

broker_url = settings.REDIS_BROKER
result_backend = settings.REDIS_BACKEND

# enable_utc = False
timezone = 'Asia/Shanghai'
task_serializer = 'json'
accept_content = ['pickle', 'json', 'msgpack', 'yaml']
result_serializer = 'json'
worker_concurrency = 1
worker_max_tasks_per_child = 3

# when add new tasks in new a module, add it below.
imports = (
    "task.capture",
    "task.offline_ticket",
    "task.clear_cache",
    # "task.export",
    "task.mail_report"
)

# add task method below
ALL_TASK = (
    "task.capture.task_run",
    "task.offline_ticket.offline_ticket",
    "task.clear_cache.clear_cache",
    # "task.export.export",
    "task.mail_report.timing_send_mail"
)

task_routes = {
    i: {'queue': i, 'routing_key': i} for i in ALL_TASK
}
task_queues = {
    Queue(i, Exchange(i), routing_key=i) for i in ALL_TASK
}
