# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleBaseTargetLoginUserStatistics"
]

from mongoengine import StringField

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

