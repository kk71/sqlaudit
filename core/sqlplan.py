# Author: kk.Fang(fkfkbill@gmail.com)

import abc


class BaseSQLPlan(abc.ABC):
    """基础SQL执行计划"""

    etl_time = None  # etl时间
