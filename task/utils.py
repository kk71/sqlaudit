# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "pending_task",
    "get_task",
    "flush_celery_q"
]

import json
from typing import NoReturn

from redis import StrictRedis

import settings
from . import const, celery_conf
from utils.conc_utils import *


redis_celery_broker = StrictRedis(
    host=settings.REDIS_BROKER_IP,
    port=settings.REDIS_BROKER_PORT,
    db=settings.REDIS_BROKER_DB
)


def pending_task() -> set:
    """获取pending的任务task_id"""
    ret = set()
    tasks = redis_celery_broker.lrange(celery_conf.task_capture_task_run, 0, -1)
    for task in tasks:
        task = json.loads(task)
        args = eval(task["headers"]["argsrepr"])
        ret.add(args[0])
    return ret


async def get_task(
        task_q,
        execution_status=None
) -> list:
    """
    获取任务列表
    :param task_q:
    :param execution_status: 为None则表示不过滤状态
    :return:
    """
    ret = []
    pending_task_ids: set = pending_task()
    with make_session() as session:
        cmdb_capture_task_latest_task_id = await async_thr(
            get_latest_task_record_id,
            status=None  # None表示不过滤状态
        )
        for t in task_q:
            t_dict = t.to_dict()
            t_dict["last_result"] = t_dict["execution_status"] = const.TASK_NEVER_RAN
            last_task_exec_record= session.query(CMDBTaskRecord).filter(
                CMDBTaskRecord.task_record_id == cmdb_capture_task_latest_task_id.get(t.cmdb_id, None)
            ).first()
            if last_task_exec_record:
                t_dict["last_result"] = last_task_exec_record.status
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


def flush_celery_q(task_type: str) -> NoReturn:
    """
    清空celery在redis的队列
    """
    redis_celery_broker.ltrim(q_name, 1, 0)
