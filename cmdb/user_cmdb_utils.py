from utils import privilege_utils
from models.sqlalchemy import make_session
from cmdb.cmdb import CMDB
from oracle_cmdb.cmdb import RoleCMDBSchema


def get_current_cmdb(user_login, id_name="cmdb_id") -> [str]:
    """
    获取某个用户可见的cmdb
    :param session:
    :param user_login:
    :param id_name: 返回的唯一键值，默认是cmdb_id，亦可选择connect_name
    :return: [cmdb_id或者connect_name, ...]
    """
    role_ids: list = list(privilege_utils.get_role_of_user(login_user=user_login).
                        get(user_login, set([])))
    with make_session() as session:
        if id_name == "cmdb_id":
            return [i[0] for i in session.query(RoleCMDBSchema.cmdb_id.distinct()).
                    join(CMDB, CMDB.cmdb_id == RoleCMDBSchema.cmdb_id).
                    filter(RoleCMDBSchema.role_id.in_(role_ids))]
        elif id_name == "connect_name":
            return [i[0] for i in session.query(CMDB.connect_name.distinct()).
                    join(RoleCMDBSchema, RoleCMDBSchema.cmdb_id == CMDB.cmdb_id).
                    filter(RoleCMDBSchema.role_id.in_(role_ids))]

