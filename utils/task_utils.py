# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "get_pending_task"
]

import json
from typing import NoReturn

from redis import StrictRedis

import settings
import celery_conf
from models.oracle import *
from utils import const, cmdb_utils, score_utils
from utils.conc_utils import *


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


async def get_task(
        session,
        task_q,
        execution_status=None
) -> list:
    """
    获取任务列表
    :param session:
    :param task_q:
    :param execution_status: 为None则表示不过滤状态
    :return:
    """
    ret = []
    pending_task_ids: set = get_pending_task()
    cmdb_capture_task_latest_task_id = await async_thr(
        score_utils.get_latest_task_record_id,
        session,
        status=None  # None表示不过滤状态
    )
    for t in task_q:
        t_dict = t.to_dict()
        t_dict["last_result"] = t_dict["execution_status"] = const.TASK_NEVER_RAN
        last_task_exec_history = session.query(TaskExecHistory).filter(
            TaskExecHistory.id == cmdb_capture_task_latest_task_id.get(t.cmdb_id, None)
        ).first()
        if last_task_exec_history:
            t_dict["last_result"] = last_task_exec_history.status
        if execution_status == const.TASK_NEVER_RAN:
            if t_dict["last_result"] is not const.TASK_NEVER_RAN:
                continue
        elif execution_status == const.TASK_PENDING:
            if t.task_id not in pending_task_ids:
                continue
        elif execution_status == const.TASK_RUNNING:
            if t_dict["last_result"] is not None:
                continue
        elif execution_status == const.TASK_DONE:
            if t_dict["last_result"] is not True:
                continue
        elif execution_status == const.TASK_FAILED:
            if t_dict["last_result"] is not False:
                continue

        if t_dict["last_result"] is None:
            t_dict["execution_status"] = const.TASK_RUNNING
        elif t.task_id in pending_task_ids:
            t_dict["execution_status"] = const.TASK_PENDING
        elif t_dict["last_result"] is True:
            t_dict["execution_status"] = const.TASK_DONE
        elif t_dict["last_result"] is False:
            t_dict["execution_status"] = const.TASK_FAILED

        ret.append(t_dict)
    return sorted(ret,
                 key=lambda k: 0 if k["last_result"] is None
                 else k["last_result"], reverse=True)


def flush_celery_q(q_name=celery_conf.task_capture_task_run) -> NoReturn:
    """
    清空celery在redis的队列
    :param q_name: 队列名，默认为采集任务队列
    :return:
    """
    redis_celery_broker.ltrim(q_name, 1, 0)
