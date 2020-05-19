# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseTargetLoginUserStatistics"
]

from typing import Generator

from mongoengine import StringField

from ...auth.user_utils import *
from ..base import OracleBaseStatistics


class OracleBaseTargetLoginUserStatistics(OracleBaseStatistics):
    """登录用户级别的统计"""

    target_login_user = StringField(required=True, null=True)

    meta = {
        "abstract": True,
        "indexes": [
            "target_login_user"
        ]
    }

    @classmethod
    def post_generated(cls, **kwargs):
        super().post_generated(**kwargs)
        target_login_user: str = kwargs["target_login_user"]
        doc: "OracleBaseTargetLoginUserStatistics" = kwargs["doc"]

        doc.target_login_user = target_login_user

    @classmethod
    def schemas(cls, session, cmdb_id: int, **kwargs) -> Generator[str, None, None]:
        login_user: str = kwargs["login_user"]
        yield from current_schema(session, login_user, cmdb_id)

