
import arrow
from typing import Union
from sqlalchemy import func
from datetime import datetime

from models.sqlalchemy import *
from cmdb.cmdb_task import CMDBTask,CMDBTaskRecord


def get_latest_task_record_id(
        cmdb_id: Union[list, int, None] = None,
        status: Union[bool, None] = True,
        task_start_date_gt: Union[datetime, callable, None] =
        lambda: arrow.now().shift(days=-7).datetime,
        task_record_id_to_replace: [dict, None] = None
) -> dict:
    """
    获取每个库最后一次采集分析的task_record_id
    :param cmdb_id:
    :param status: 是否指定结束状态？True表示只过滤成功的，False表示失败，None表示不过滤
    :param task_start_date_gt: 搜索的task_record必须晚于某个时间点(datetime)
    :param task_record_id_to_replace: 提供一个替换的{cmdb_id: record_id}
    :return: {cmdb_id: task_record_id, ...}
    """
    if callable(task_start_date_gt):
        task_start_date_gt: Union[datetime, None] = task_start_date_gt()
    with make_session() as session:
        sub_q = session.query(CMDBTaskRecord.task_record_id.label("id"),CMDBTask.cmdb_id.label("cmdb_id")).\
            join(CMDBTaskRecord,CMDBTaskRecord.connect_name == CMDBTask.connect_name)
        if cmdb_id:
            if not isinstance(cmdb_id, (tuple, list)):
                cmdb_id = [cmdb_id]
            sub_q = sub_q.filter(CMDBTask.cmdb_id.in_(cmdb_id))
        if status is not None:
            sub_q = sub_q.filter(CMDBTask.status == status)
        if task_start_date_gt is not None:
            sub_q = sub_q.filter(CMDBTaskRecord.create_time > task_start_date_gt)
        sub_q = sub_q.subquery()
        cmdb_id_exec_hist_id_list_q = session. \
            query(sub_q.c.cmdb_id, func.max(sub_q.c.id)).group_by(sub_q.c.cmdb_id)
        ret = dict(list(cmdb_id_exec_hist_id_list_q))
        if task_record_id_to_replace:
            ret.update(task_record_id_to_replace)
        return ret