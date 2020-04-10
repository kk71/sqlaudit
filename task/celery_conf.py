# Author: kk.Fang(fkfkbill@gmail.com)

from celery import platforms

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

imports = []
task_routes = {}
task_queues = set()

# 指定需要搜索任务的packages

PACKAGES_TO_SEARCH_FOR_TASKS = (
    "task",
)
