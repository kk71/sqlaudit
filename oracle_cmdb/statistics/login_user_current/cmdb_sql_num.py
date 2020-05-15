# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsCMDBSQLNum"
]

from typing import Union, Generator

from mongoengine import IntField, ListField

from utils.datetime_utils import *
from models.sqlalchemy import *
from ...capture import OracleSQLText
from ...issue.sql import OracleOnlineSQLIssue
from ...tasks.capture.cmdb_task_capture import *
from ..base import *
from .base import *


@OracleBaseStatistics.need_collect()
class OracleStatsCMDBSQLNum(OracleStatsMixOfLoginUserAndCurrentCMDB):
    """登录用户与当前库的SQL数量统计"""

    DATE_PERIOD = (7, 30)  # 数据天数可供选项

    date_period = IntField(help_text="时间区间", choices=DATE_PERIOD)
    active = ListField(default=list)  # [{date: value}, ...]
    at_risk = ListField(default=list)

    meta = {
        "collection": "oracle_stats_cmdb_sql_num",
        "indexes": [
            "date_period"
        ]
    }

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsCMDBSQLNum", None, None]:
        period_now = arrow.now()
        with make_session() as session:
            for the_user in cls.users(session, cmdb_id=cmdb_id):
                for the_cmdb in cls.cmdbs(session, cmdb_id=cmdb_id):
                    # the_cmdb虽然循环了，但是这里只能拿到当前任务的库

                    for date_period in cls.DATE_PERIOD:
                        doc = cls(date_period=date_period)
                        the_cmdb_task = OracleCMDBTaskCapture.get_cmdb_task_by_cmdb(
                            the_cmdb)
                        date_tri = the_cmdb_task.day_last_succeed_task_record_id(
                            date_start=period_now.shift(days=-date_period),
                            date_end=period_now,
                            task_record_id_supposed_to_be_succeed=task_record_id
                        )
                        for d, the_task_record_id in date_tri.items():

                            sql_qs = OracleSQLText.filter(
                                task_record_id=the_task_record_id
                            )
                            doc.active.append({
                                "date": d_to_str(d),
                                "value": sql_qs.count()
                            })

                            sql_id_with_issue = OracleOnlineSQLIssue.filter(
                                task_record_id=the_task_record_id
                            ).distinct("sql_id")
                            doc.at_risk.append({
                                "date": d_to_str(d),
                                "value": len(sql_id_with_issue)
                            })
                        cls.post_generated(
                            doc=doc,
                            task_record_id=task_record_id,
                            cmdb_id=cmdb_id,
                            target_login_user=the_user.login_user
                        )
                        yield doc

