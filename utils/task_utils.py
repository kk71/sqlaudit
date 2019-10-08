# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "get_pending_task"
]

import json
from typing import NoReturn

from redis import StrictRedis

import settings
import celery_conf


redis_celery_broker = StrictRedis(
    host=settings.REDIS_BROKER_IP,
    port=settings.REDIS_BROKER_PORT,
    db=settings.REDIS_BROKER_DB
)


def get_pending_task() -> set:
    """获取pending的任务task_id"""
    ret = set()
    tasks = redis_celery_broker.lrange(celery_conf.task_capture_task_run, 0, -1)
    for task in tasks:
        task = json.loads(task)
        args = eval(task["headers"]["argsrepr"])
        ret.add(args[5])
    return ret


def flush_celery_q(q_name=celery_conf.task_capture_task_run) -> NoReturn:
    """
    清空celery在redis的队列
    :param q_name: 队列名，默认为采集任务队列
    :return:
    """
    redis_celery_broker.ltrim(q_name, 1, 0)
