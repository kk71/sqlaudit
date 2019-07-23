# Author: kk.Fang(fkfkbill@gmail.com)
"""重构的采集模块"""

__all__ = [
    "capture"
]

from typing import Union

from utils.perf_utils import timing
from plain_db.oracleob import OracleOB
from models.oracle import CMDB, make_session
from models.mongo import *
from models.mongo.utils import *

# 普通采集涉及的模块
CAPTURE_ITEMS = (
    ObjSeqInfo,
    ObjTabSpace
)

# 实时采集涉及的模块
REALTIME_CAPTURE_ITEMS = ()


@timing()
def capture(
        task_record_id,
        cmdb_id,
        schema_name: Union[None, str],
        capture_type: Union[SchemaCapture, CMDBCapture]):
    """
    采集
    :param task_record_id:
    :param cmdb_id:
    :param schema_name:
    :param capture_type:
    :return:
    """
    with make_session() as session:
        cmdb = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).first()
        cmdb_conn = OracleOB(
            host=cmdb.ip_address,
            port=cmdb.port,
            username=cmdb.user_name,
            password=cmdb.password,
            sid=cmdb.service_name,
            # service_name=cmdb.sid
        )
        for m in CAPTURE_ITEMS:
            if not issubclass(m, capture_type):
                continue
            print(f"* starting {capture_type.__doc__} for {m.__doc__} ...")
            sql = m.command_to_execute(schema_name)
            print(f"Going to capture: {sql}")
            docs = [m(**c) for c in cmdb_conn.select_dict(sql, one=False)]
            m.post_captured(docs, cmdb_id, task_record_id, schema_name)
            if not docs:
                print("no objects captured.")
                continue
            m.objects.insert(docs)


@timing()
def realtime_capture(cmdb_id, schema_name):
    """
    实时采集
    :param cmdb_id:
    :param schema_name:
    :return:
    """
    raise NotImplementedError
