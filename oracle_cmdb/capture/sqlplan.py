# Author: kk.Fang(fkfkbill@gmail.com)

from .base import SQLCapturingDoc


class SQLPlan(SQLCapturingDoc):
    """纳管库sql执行计划"""

    @classmethod
    def capture_sql(cls, schema_name: str, sql_id: str, **kwargs):
        return
