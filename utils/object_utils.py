# Author: kk.Fang(fkfkbill@gmail.com)

from sqlalchemy.orm.session import Session

from models.oracle import *
from models.mongo import *
from utils.perf_utils import *
from utils.cache_utils import *


@timing()
@cache_it(cache=sc, type_to_exclude=Session)
def get_cmdb_phy_size(session, cmdb) -> int:
    """
    计算cmdb最近一次统计的物理体积（目前仅计算表的总体积）
    :param session:
    :param cmdb:
    :return: int
    """
    # TODO make it cached
    latest_task_exec_hist_obj = session.query(TaskExecHistory). \
        filter(TaskExecHistory.connect_name == cmdb.connect_name). \
        order_by(TaskExecHistory.task_end_date.desc()).first()
    if latest_task_exec_hist_obj:
        ret = list(ObjTabInfo.objects.aggregate(
            {
                "$match": {
                    "record_id": {"$regex": f"{latest_task_exec_hist_obj.id}"},
                    "cmdb_id": cmdb.cmdb_id
                }
            },
            {
                "$group": {
                    "_id": "$cmdb_id",
                    "sum": {"$sum": "$PHY_SIZE(MB)"}
                }
            }
        ))
        if ret:
            return ret[0]["sum"]
