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
                    for the_schema_name in cls.schemas(
                            session,
                            cmdb_id=the_cmdb.cmdb_id,
                            login_user=the_user.login_user):
                        for issue_model in cls.ISSUES:
                            doc = cls()
                            issue_q = OracleOnlineIssue.filter(
                                task_record_id=the_cmdb.cmdb_task(session),
                                schema_name=the_schema_name,
                                entries__all=issue_model.ENTRIES
                            )
                            doc.problem_num += issue_q.count()
                            related_capture_models = \
                                OracleOnlineIssue.related_capture(
                                    issue_model.ENTRIES)
                            for rcm in related_capture_models:
                                captured_q = rcm.filter(
                                    # todo 这里必须要用model.filter
                                    task_record_id=the_cmdb.cmdb_task(session),
                                    schema_name=the_schema_name,
                                    entries__all=issue_model.ENTRIES
                                )
                                doc.num += captured_q.count()
                                doc.num_with_risk += issue_model.referred_capture(
                                    rcm, issue_qs=captured_q).count()
                            yield doc
