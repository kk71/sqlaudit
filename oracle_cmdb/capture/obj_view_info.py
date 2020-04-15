# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ObjViewInfo"
]

from mongoengine import StringField

from .base import SchemaObjectCapturingDoc


@SchemaObjectCapturingDoc.need_collect()
class ObjViewInfo(SchemaObjectCapturingDoc):
    """视图信息"""

    obj_pk = StringField()
    owner = StringField()
    view_name = StringField()
    object_type = StringField()
    referenced_owner = StringField()
    referenced_name = StringField()
    referenced_type = StringField()

    meta = {
        "collection": "obj_view_info",
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
