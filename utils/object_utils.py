# Author: kk.Fang(fkfkbill@gmail.com)

from models.oracle import *
from models.mongo import *
from utils.perf_utils import *


@timing
def get_cmdb_phy_size(session, cmdb) -> int:
    """
    计算cmdb最近一次统计的物理体积（目前仅计算表的总体积）
    :param session:
    :param cmdb:
    :return: int
    """
    # TODO make it cached
    phy_size = None
    latest_task_exec_hist_obj = session.query(TaskExecHistory). \
        filter(TaskExecHistory.connect_name == cmdb.connect_name). \
        order_by(TaskExecHistory.task_end_date.desc()).first()
    if latest_task_exec_hist_obj:
        tab_phy_size_iter = ObjTabInfo.\
            filter_by_exec_hist_id(str(latest_task_exec_hist_obj.id)). \
            filter(cmdb_id=cmdb.cmdb_id).values_list("phy_size_mb")
        phy_size = 0
        if tab_phy_size_iter:
            phy_size = sum([i for i in tab_phy_size_iter if i])
    return phy_size
