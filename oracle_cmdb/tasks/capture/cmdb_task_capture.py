# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleCMDBTaskCapture"
]

from typing import Tuple, Dict, Any, Optional, Union, List

import task.const
import cmdb.cmdb_task
import rule.const
from models.sqlalchemy import *
from ...cmdb import *
from ...statistics import *


class OracleCMDBTaskCapture(cmdb.cmdb_task.CMDBTask):
    """oracle纳管库采集分析任务"""

    __mapper_args__ = {
        'polymorphic_identity': task.const.TASK_TYPE_CAPTURE
    }

    @classmethod
    def get_cmdb_task_by_cmdb(
            cls,
            the_cmdb: OracleCMDB,
            **kwargs) -> "OracleCMDBTaskCapture":
        return super().get_cmdb_task_by_cmdb(
            the_cmdb, **kwargs).filter(
            cls.task_type == task.const.TASK_TYPE_CAPTURE).first()

    @classmethod
    def query_cmdb_task_with_last_record(
            cls,
            session,
            **kwargs) -> Tuple[sqlalchemy_q, QueryEntity]:
        kwargs["task_type"] = task.const.TASK_TYPE_CAPTURE
        return super().query_cmdb_task_with_last_record(session, **kwargs)

    @classmethod
    def last_cmdb_score(cls, session, **kwargs) -> Dict[int, Dict[str, Optional[Any]]]:
        """
        查询纳管库最近一次评分信息
        """
        cmdb_id_task_record_id: Dict[int, int] = cls.last_success_task_record_id_dict(
            session, **kwargs)
        cmdb_score_q = OracleStatsCMDBScore.filter(
            task_record_id__in=cmdb_id_task_record_id.values()
        )
        cmdb_score_dict: Dict[int, dict] = {
            i.cmdb_id: i.to_dict()
            for i in cmdb_score_q
        }
        return {
            # 能够确保如果某个库没有数据，那么该库的字段不会不存在
            cmdb_id: cmdb_score_dict.get(cmdb_id, {})
            for cmdb_id in cmdb_id_task_record_id.keys()
        }

    @classmethod
    def last_cmdb_main_score(cls, session, **kwargs) -> Dict[int, float]:
        """只查采集询纳管库任务的主得分"""
        last_cmdb_score = cls.last_cmdb_score(session, **kwargs)
        return {
            cmdb_id: score_dict.get("entry_score", {}).get(rule.const.RULE_ENTRY_ONLINE)
            if isinstance(score_dict, dict) else None
            for cmdb_id, score_dict in last_cmdb_score.items()
        }

    @classmethod
    def last_login_user_entry_cmdb(
            cls,
            session,
            the_login_user: str,
            entry: Optional[Union[str, List[str]]] = None,
            **kwargs) -> Dict[int, Dict[str, Any]]:
        """最近一次登录用户绑定的各个纳管库层面各个维度的统计信息累计和，不按照schema区分"""
        cmdb_id_task_record_id: Dict[int, int] = cls.last_success_task_record_id_dict(
            session, **kwargs)
        entry_cmdb_q = OracleStatsEntryCMDB.filter(
            target_login_user=the_login_user,
            task_record_id__in=list(cmdb_id_task_record_id.values())
        )
        if entry is not None:
            if isinstance(entry, str):
                entry = [entry]
            elif isinstance(entry, (list, tuple)):
                pass
            else:
                assert 0
        if entry:
            entry_cmdb_q = entry_cmdb_q.filter(entry__in=entry)
        ret = {}
        for cmdb_id in cmdb_id_task_record_id.keys():
            ret[cmdb_id] = {
                i.entry: i.to_dict()
                for i in entry_cmdb_q.filter(target_cmdb_id=cmdb_id)
            }
        return ret
