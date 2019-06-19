# Author: kk.Fang(fkfkbill@gmail.com)

from celery import platforms
from kombu import Exchange, Queue

import settings


platforms.C_FORCE_ROOT = True

broker_url = settings.REDIS_BROKER
result_backend = settings.REDIS_BACKEND

timezone = 'Asia/Shanghai'
task_serializer = 'json'
accept_content = ['pickle', 'json', 'msgpack', 'yaml']
result_serializer = 'json'
worker_concurrency = 2
worker_max_tasks_per_child = 3

# when add new tasks in new a module, add it below.
imports = (
    "task.capture",
    "task.sqlaitune",
    "task.offline_ticket")

# when add a new task, add it to blow.
_capture = "task.capture.task_run"
# _aitune = "task.sqlaitune.sqlaitune_run"
_submit_ticket = "task.offline_ticket.offline_ticket"

ALL_TASK = (
    _capture,
    # _aitune,
    _submit_ticket
)

task_routes = {
    i: {'queue': i, 'routing_key': i} for i in ALL_TASK
}
task_queues = {
    Queue(i, Exchange(i), routing_key=i) for i in ALL_TASK
}
