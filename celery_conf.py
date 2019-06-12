# Author: kk.Fang(fkfkbill@gmail.com)

from kombu import Exchange, Queue
from celery import platforms

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
imports = ("task.capture", "task.offline_ticket")

# when add a new task, add it to blow.
_capture = "task.capture.task_run"
_submit_ticket = "task.offline_ticket.offline_ticket"

task_routes = {
    _capture: {
        'queue': _capture, 'routing_key': _capture},
    _submit_ticket: {
        'queue': _submit_ticket, 'routing_key': _submit_ticket},
}
task_queues = {
    Queue(_capture, Exchange(_capture), routing_key=_capture),
    Queue(_submit_ticket, Exchange(_submit_ticket), routing_key=_submit_ticket),
}
