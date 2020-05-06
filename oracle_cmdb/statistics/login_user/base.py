# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Generator

from mongoengine import StringField

from models.sqlalchemy import *
from auth.user import User
from oracle_cmdb.statistics import OracleBaseStatistics


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
    def users(cls, session) -> Generator[User, None, None]:
        """获取login_user列表"""
        yield from session.query(User)
