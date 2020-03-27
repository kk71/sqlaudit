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
worker_max_tasks_per_child = 1
worker_prefetch_multiplier = 1
task_acks_late = True
broker_transport_options = {'visibility_timeout': 60*60*24*9}

# when add new tasks in new a module, add it below.
imports = (
    "task.capture",
    # "task.offline_ticket",
    "oracle_cmdb.ticket.task"
    "task.clear_cache",
    # "task.export",
    "task.mail_report"
)

# add task method below
task_capture_task_run = "task.capture.task_run"
ALL_TASK = (
    task_capture_task_run,
    # "task.offline_ticket.offline_ticket",
    "oracle_cmdb.ticket.task.ticket_analyse",
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
