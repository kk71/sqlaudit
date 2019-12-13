# Author: kk.Fang(fkfkbill@gmail.com)

from redis import StrictRedis

import settings
from utils import const
from utils.datetime_utils import *


cache_redis_cli = StrictRedis(
    host=settings.CACHE_REDIS_IP,
    port=settings.CACHE_REDIS_PORT,
    db=settings.CACHE_REDIS_DB
)


def get_current_offline_ticket_task_name(submit_owner):
    """获取当前可用的线下审核任务名"""
    current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
    k = f"offline-ticket-task-num-{current_date}"
    current_num = "%03d" % cache_redis_cli.incr(k, 1)
    cache_redis_cli.expire(k, 60*60*24*3)  # 设置三天内超时
    return f"{submit_owner}-{current_date}-{current_num}"
