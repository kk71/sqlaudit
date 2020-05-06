# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "current_cmdb",
    "current_schema"
]

from typing import Union

import auth.utils
from models.sqlalchemy import make_session, QueryEntity
from auth.user import *
from ..cmdb import *
from .role import *


def current_cmdb(login_user: str) -> [int]:
    """获取某个用户可见的cmdb_id"""
    role_ids: list = list(auth.utils.role_of_user(login_user=login_user).
                          get(login_user, set([])))
    with make_session() as session:
        return [
            i[0]
            for i in session.query(RoleOracleCMDBSchema.cmdb_id.distinct()).
            join(OracleCMDB, OracleCMDB.cmdb_id == RoleOracleCMDBSchema.cmdb_id).
            filter(RoleOracleCMDBSchema.role_id.in_(role_ids))
        ]


def current_schema(
        login_user: Union[str, list, tuple, None] = None,
        cmdb_id: int = None,
        verbose: bool = False,
        verbose_dict: bool = False,
        query_entity: Union[tuple, list] = ()) -> list:
    """
    获取某个用户可见的schema
    :param login_user: 注意，如果传过来是多个login_user,并且返回不是verbose==True，
                        并且还没传cmdb_id，
                        那么意味着多个库有重复的schema会被合并
    :param cmdb_id: 为None则表示拿全部绑定的schema
    :param verbose:
    :param verbose_dict:
    :param query_entity: 需要查询的字段
    :return: verbose==False:[schema_name, ...]
             verbose==True: [(*query_entity), ...]
             verbose_dict==True: [{*query_entity: values}, ...]
             所有返回结果都是list，并且去重。
    """
    with make_session() as session:
        qe = QueryEntity(*query_entity)
        if verbose or verbose_dict:
            q = session.query(*qe)
            models_to_join = set()
            for a_qe in qe:
                if a_qe.class_ in (OracleCMDB, Role):
                    models_to_join.add(a_qe.class_)
                elif a_qe.class_ == RoleOracleCMDBSchema:
                    pass
                else:
                    assert 0
            for m in models_to_join:
                if m == OracleCMDB:
                    q = q.join(OracleCMDB,
                               OracleCMDB.cmdb_id == RoleOracleCMDBSchema.cmdb_id)
                if m == Role:
                    q = q.join(Role, Role.role_id == RoleOracleCMDBSchema.role_id)
        else:
            q = session.query(RoleOracleCMDBSchema.schema_name.distinct())
    if login_user:
        role_ids: list = list(auth.utils.role_of_user(login_user=login_user).
                              get(login_user, set([])))
        q = q.filter(RoleOracleCMDBSchema.role_id.in_(role_ids))
    if cmdb_id:
        q = q.filter(RoleOracleCMDBSchema.cmdb_id == cmdb_id)
    if verbose:
        return list(set(q))
    elif verbose_dict:
        return [qe.to_dict(i) for i in set(q)]
    else:
        return [i[0] for i in q]
