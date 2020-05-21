# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "get_available_task_name"
]

from redis import StrictRedis

import settings
from utils.datetime_utils import *
from ticket.ticket import Ticket


redis_cli = StrictRedis(
    host=settings.CACHE_REDIS_IP,
    port=settings.CACHE_REDIS_PORT,
    db=settings.CACHE_REDIS_DB
)


def get_available_task_name(submit_owner: str) -> str:
    """获取当前可用的线下审核任务名"""
    current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
    k = f"ticket-task-num-{current_date}"
    current_num_int = redis_cli.incr(k, 1)
    current_num = "%03d" % current_num_int
    redis_cli.expire(k, 60 * 60 * 24 * 3)  # 设置三天内超时
    ret = f"{submit_owner}-{current_date}-{current_num}"
    if current_num_int == 1:
        while Ticket.filter(task_name=ret).count():
            current_num_int = redis_cli.incr(k, 1)
            current_num = "%03d" % current_num_int
            redis_cli.expire(k, 60 * 60 * 24 * 3)  # 设置三天内超时
            ret = f"{submit_owner}-{current_date}-{current_num}"
    return ret

