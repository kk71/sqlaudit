# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsDashboardDrillDown"
]

from typing import Union, Generator

from mongoengine import LongField, FloatField

from ..current_task.schema_rate import *
from ...issue import *
from .base import *


@OracleBaseStatistics.need_collect()
class OracleStatsDashboardDrillDown(OracleStatsMixOfLoginUserAndTargetSchema):
    """仪表盘下钻数据"""

    num = LongField(default=0, help_text="采集到的总数")
    num_with_risk = LongField(default=0, help_text="有问题的采到的个数")
    # num_with_risk_rate = FloatField(help_text="有问题的采到的个数rate")
    problem_num = LongField(default=0, help_text="问题个数")
    # problem_num_rate = FloatField(help_text="问题个数rate(风险率)")
    score = FloatField(default=0)

    meta = {
        "collection": "oracle_stats_dashboard_drill_down"
    }

    REQUIRES = (OracleStatsSchemaRate,)

    ISSUES = (
        OracleOnlineSQLTextIssue,
        OracleOnlineSQLPlanIssue,
        OracleOnlineSQLStatIssue,
        OracleOnlineObjectIssueTable,
        OracleOnlineObjectIssueSequence,
        OracleOnlineObjectIssueIndex
    )

    @classmethod
    def schemas(cls, session, cmdb_id: int, **kwargs) -> Generator[str, None, None]:
        login_user: str = kwargs["login_user"]
        yield from current_schema(session, login_user, cmdb_id)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsDashboardDrillDown", None, None]:
        with make_session() as session:
            for the_user in cls.users(session):
                for the_cmdb in cls.cmdbs(session, login_user=the_user.login_user):

                    # 当前库的最后一次成功采集
                    the_cmdb_last_success_task_record_id = the_cmdb.cmdb_task(
                        session).last_success_task_record_id
                    if not the_cmdb_last_success_task_record_id:
                        continue  # 如果一次成功都没有则忽略当前库

                    for the_schema_name in cls.schemas(
                            session,
                            cmdb_id=the_cmdb.cmdb_id,
                            login_user=the_user.login_user):
                        for issue_model in cls.ISSUES:
                            doc = cls()
                            issue_q = OracleOnlineIssue.filter(
                                task_record_id=the_cmdb_last_success_task_record_id,
                                schema_name=the_schema_name,
                                entries__all=issue_model.ENTRIES
                            )
                            # 计算问题数量
                            doc.problem_num += issue_q.count()

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
                                doc.num_with_risk += issue_model.referred_capture(
                                    rcm, issue_qs=captured_q).count()

                            # 从schema分数统计表取得分数
                            the_score_stats = OracleStatsSchemaRate.filter(
                                task_record_id=the_cmdb_last_success_task_record_id,
                                schema_name=the_schema_name
                            ).first()
                            if the_score_stats:
                                doc.score = the_score_stats.score_average  # 使用平均分

                            cls.post_generated(
                                doc=doc,
                                target_login_user=the_user.login_user,
                                target_schema_name=the_schema_name,
                                target_cmdb_id=the_cmdb.cmdb_id,
                                target_task_record_id=the_cmdb_last_success_task_record_id
                            )
                            yield doc
