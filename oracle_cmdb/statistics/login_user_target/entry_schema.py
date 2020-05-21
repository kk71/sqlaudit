# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsEntrySchema"
]

from typing import Union, Generator

from mongoengine import LongField, FloatField, ListField, StringField

from models.sqlalchemy import *
from ..current_task.schema_score import *
from ...issue import *
from .base import *
from ..base import *


@OracleBaseStatistics.need_collect()
class OracleStatsEntrySchema(OracleStatsMixOfLoginUserAndTargetSchema):
    """登录用户各库各schema各维度对象数问题数和风险率"""

    entry = StringField()
    entries = ListField()
    num = LongField(default=0)  # 采集到的[sql/object]总数（去重）
    num_with_risk = LongField(default=0)  # 包含问题的[sql/object]数（去重）
    risk_rate = FloatField(default=0)  # num_with_risk/num
    issue_num = LongField(default=0)  # 问题数
    score = FloatField(default=0)

    meta = {
        "collection": "oracle_stats_entry_schema",
        "indexes": [
            "entry",
            "entries"
        ]
    }

    REQUIRES = (OracleStatsSchemaScore,)

    ISSUES = (
        OracleOnlineSQLIssue,
        OracleOnlineSQLTextIssue,
        OracleOnlineSQLPlanIssue,
        OracleOnlineSQLStatIssue,
        OracleOnlineObjectIssue,
        OracleOnlineObjectIssueTable,
        OracleOnlineObjectIssueSequence,
        OracleOnlineObjectIssueIndex
    )

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsEntrySchema", None, None]:
        with make_session() as session:
            for the_user in cls.users(session):
                for the_cmdb in cls.cmdbs(session, login_user=the_user.login_user):

                    the_cmdb_last_success_task_record_id = \
                        cls.cmdb_latest_available_task_record_id_for_stats(
                            cmdb_id=cmdb_id,
                            task_record_id=task_record_id,
                            target_cmdb=the_cmdb
                        )
                    if not the_cmdb_last_success_task_record_id:
                        continue  # 如果一次成功都没有则忽略当前库

                    for the_schema_name in cls.schemas(
                            session,
                            cmdb_id=the_cmdb.cmdb_id,
                            login_user=the_user.login_user):
                        for issue_model in cls.ISSUES:
                            # 假设这里的entries元组长度为1
                            doc = cls(
                                entry=issue_model.ENTRIES[0],  # 假设只有一个entry
                                entries=issue_model.INHERITED_ENTRIES
                            )
                            issue_q = OracleOnlineIssue.filter(
                                task_record_id=the_cmdb_last_success_task_record_id,
                                schema_name=the_schema_name,
                                entries__all=issue_model.ENTRIES
                            )
                            # 计算问题数量
                            doc.issue_num += issue_q.count()

                            # 计算问题对应的原始对象/sql数量（亦即采集到的有问题的数目）
                            related_capture_models = \
                                OracleOnlineIssue.related_capture(
                                    issue_model.ENTRIES)
                            for rcm in related_capture_models:
                                captured_q = rcm.filter(
                                    # todo 这里必须要用model.filter
                                    task_record_id=the_cmdb_last_success_task_record_id,
                                    schema_name=the_schema_name
                                )
                                doc.num += captured_q.count()
                                doc.num_with_risk += len(issue_model.referred_capture_distinct(
                                    rcm, issue_qs=issue_q))
                            if doc.num_with_risk > doc.num:
                                print(f"warning: {the_schema_name=} {issue_model=} {doc.num=} {doc.num_with_risk=}")
                                # 偷偷的修正一下这个数字~
                                doc.num_with_risk = doc.num
                            if doc.num:
                                doc.risk_rate = doc.num_with_risk / doc.num

                            # 从schema分数统计表取得分数
                            the_score_stats = OracleStatsSchemaScore.filter(
                                task_record_id=the_cmdb_last_success_task_record_id,
                                schema_name=the_schema_name
                            ).first()
                            if the_score_stats:
                                doc.score = the_score_stats.entry_score.get(doc.entry, None)

                            cls.post_generated(
                                doc=doc,
                                task_record_id=task_record_id,
                                cmdb_id=cmdb_id,
                                target_login_user=the_user.login_user,
                                target_schema_name=the_schema_name,
                                target_cmdb_id=the_cmdb.cmdb_id,
                                target_task_record_id=the_cmdb_last_success_task_record_id
                            )
                            yield doc
