# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "get_pending_task"
]

import json

from celery.task.control import inspect

import celery_conf
from utils.perf_utils import timing


@timing()
def get_pending_task():
    """获取pending的任务task_id"""
    insp = inspect()
    q_name = None
    for exch_id, exchanges in insp.active_queues().items():
        if not exchanges:
            continue
        the_exch = exchanges[0]
        if the_exch["name"] == celery_conf.task_capture_task_run:
            q_name = exch_id
            break
    if not q_name:
        assert 0
    ret = set()
    for t in insp.reserved()[q_name]:
        try:
            ret.add(json.loads(t["args"])[5])
        except json.JSONDecodeError:
            pass
    return ret
