# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleStatsDashboardDrilldown"
]

from typing import Union, Generator

from mongoengine import LongField, FloatField

import rule.const
from ..current_task.schema_rate import *
from ...issue import OracleOnlineIssue
from .base import *


@OracleBaseStatistics.need_collect()
class OracleStatsDashboardDrilldown(OracleStatsMixOfLoginUserAndTargetSchema):
    """仪表盘下钻数据"""

    num = LongField(default=0, help_text="采集到的总数")
    num_with_risk = LongField(default=0, help_text="有问题的采到的个数")
    # num_with_risk_rate = FloatField(help_text="有问题的采到的个数rate")
    problem_num = LongField(default=0, help_text="问题个数")
    # problem_num_rate = FloatField(help_text="问题个数rate(风险率)")
    score = FloatField(default=0)

    meta = {
        "collection": "oracle_stats_dashboard_drilldown"
    }

    REQUIRES = (OracleStatsSchemaRate,)

    ENTRIES_TO_CALC = (rule.const.RULE_ENTRY_ONLINE_SQL_TEXT,
                       rule.const.RULE_ENTRY_ONLINE_SQL_PLAN,
                       rule.const.RULE_ENTRY_ONLINE_SQL_STAT,
                       rule.const.RULE_ENTRY_ONLINE_TABLE,
                       rule.const.RULE_ENTRY_ONLINE_SEQUENCE,
                       rule.const.RULE_ENTRY_ONLINE_INDEX)

    @classmethod
    def schemas(cls, session, cmdb_id: int, **kwargs) -> Generator[str, None, None]:
        login_user: str = kwargs["login_user"]
        yield from current_schema(session, login_user, cmdb_id)

    @classmethod
    def generate(
            cls,
            task_record_id: int,
            cmdb_id: Union[int, None],
            **kwargs) -> Generator["OracleStatsDashboardDrilldown", None, None]:
        with make_session() as session:
            for the_user in cls.users(session):
                for the_cmdb in cls.cmdbs(session, login_user=the_user.login_user):
                    for the_schema_name in cls.schemas(
                            session,
                            cmdb_id=the_cmdb.cmdb_id,
                            login_user=the_user.login_user):
                        for the_entry in cls.ENTRIES_TO_CALC:
                            doc = cls()
                            issue_q = OracleOnlineIssue.filter(
                                task_record_id=the_cmdb.cmdb_task(session),
                                schema_name=the_schema_name,
                                entries=the_entry
                            )
                            doc.problem_num += issue_q.count()
                            related_models = OracleOnlineIssue.related_capture(
                                [the_entry])
                            for rm in related_models:
                                captured_q = rm.filter(
                                    # todo 这里必须要用model.filter
                                    task_record_id=the_cmdb.cmdb_task(session),
                                    schema_name=the_schema_name,
                                    entries=the_entry
                                )
                                doc.num += captured_q.count()
                            yield doc
