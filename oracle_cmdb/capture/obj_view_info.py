# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleObjViewInfo"
]

from typing import Tuple

from mongoengine import StringField

from .. import const
from .base import OracleSchemaObjectCapturingDoc


@OracleSchemaObjectCapturingDoc.need_collect()
class OracleObjViewInfo(OracleSchemaObjectCapturingDoc):
    """视图信息"""

    obj_pk = StringField(null=True)
    owner = StringField(null=True)
    view_name = StringField(null=True)
    object_type = StringField(null=True)
    referenced_owner = StringField(null=True)
    referenced_name = StringField(null=True)
    referenced_type = StringField(null=True)

    meta = {
        "collection": "oracle_obj_view_info",
        "indexes": [
            "obj_pk",
            "owner",
            "view_name"
        ]
    }

    @classmethod
    def simple_capture(cls, **kwargs) -> str:
        obj_owner: str = kwargs["obj_owner"]
        return f"""
     select name||referenced_name as obj_pk,
     s.owner,
     s.name as view_name,
     s.type as object_type,
     s.referenced_owner,
     s.referenced_name,
     s.referenced_type
    from DBA_DEPENDENCIES s
    where s.type = 'VIEW'
    and s.owner = '{obj_owner}'
    order by s.name
"""

    def get_object_unique_name(self) -> Tuple[str, str, str]:
        return self.owner, const.ORACLE_OBJECT_TYPE_VIEW, self.view_name
