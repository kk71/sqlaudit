
from typing import Union

from cmdb.cmdb import *
from auth.user import *
from auth import privilege_utils
from oracle_cmdb.cmdb import RoleCMDBSchema
from models.sqlalchemy import make_session, QueryEntity


def get_current_schema(
        user_login: Union[str, list, tuple, None] = None,
        cmdb_id=None,
        verbose: bool = False,
        verbose_dict: bool = False,
        query_entity: Union[tuple, list] = ()) -> list:
    """
    获取某个用户可见的schema
    :param session:
    :param user_login: 注意，如果传过来是多个login_user,并且返回不是verbose==True，并且还没传cmdb_id，
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
                if a_qe.class_ in (CMDB, Role):
                    models_to_join.add(a_qe.class_)
                elif a_qe.class_ == RoleCMDBSchema:
                    pass
                else:
                    assert 0
            for m in models_to_join:
                if m == CMDB:
                    q = q.join(CMDB, CMDB.cmdb_id == RoleCMDBSchema.cmdb_id)
                if m == Role:
                    q = q.join(Role, Role.role_id == RoleCMDBSchema.role_id)
        else:
            q = session.query(RoleCMDBSchema.schema_name.distinct())
    if user_login:
        role_ids: list = list(privilege_utils.get_role_of_user(login_user=user_login).
                              get(user_login, set([])))
        q = q.filter(RoleCMDBSchema.role_id.in_(role_ids))
    if cmdb_id:
        q = q.filter(RoleCMDBSchema.cmdb_id == cmdb_id)
    if verbose:
        return list(set(q))
    elif verbose_dict:
        return [qe.to_dict(i) for i in set(q)]
    else:
        return [i[0] for i in q]