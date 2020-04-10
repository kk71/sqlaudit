# Author: kk.Fang(fkfkbill@gmail.com)

# old oracle data
from utils.migrate_from_oracle import make_oracle_session
from utils.migrate_from_oracle import User as OUser, UserRole as OUserRole, \
    RolePrivilege as ORolePrivilege, Role as ORole, CMDB as OCMDB, \
    RoleDataPrivilege as ORoleDataPrivilege, \
    DataHealthUserConfig as ODataHealthUserConfig

# new mysql data
from models import init_models

init_models()

import cmdb.const
from models.sqlalchemy import make_session
from auth.user import *
from oracle_cmdb.cmdb import *
from oracle_cmdb.rate import *
from task.task import *


def main():
    """oracle data migrate to mysql"""
    with make_oracle_session() as o_session:
        o_user_q = o_session.query(OUser)
        o_user_role_q = o_session.query(OUserRole)
        o_role_privilege = o_session.query(ORolePrivilege)
        o_role = o_session.query(ORole)
        o_cmdb = o_session.query(OCMDB)
        o_role_data_privilege = o_session.query(ORoleDataPrivilege)
        o_data_health_user = o_session.query(ODataHealthUserConfig)

        with make_session() as m_session:
            for x in o_user_q:
                x = x.to_dict()
                x.pop('col')
                x['username'] = x.pop('user_name')
                x['create_time'] = x.pop('create_date')
                m_user = User(**x)
                m_session.add(m_user)
            for x in o_user_role_q:
                x = x.to_dict()
                x['create_time'] = x.pop('create_date')
                m_user_role = UserRole(**x)
                m_session.add(m_user_role)
            for x in o_role_privilege:
                x = x.to_dict()
                x.pop("id")
                x['create_time'] = x.pop('create_date')
                m_role_privilege = RolePrivilege(**x)
                m_session.add(m_role_privilege)
            for x in o_role:
                x = x.to_dict()
                x['create_time'] = x.pop('create_date')
                m_role = Role(**x)
                m_session.add(m_role)
            for x in o_cmdb:
                x = x.to_dict()
                x.pop('auto_sql_optimized')
                x.pop('white_list_status')
                x.pop('machine_room')
                x.pop('while_list_rule_counts')
                x.pop('is_collect')
                x.pop('create_owner')
                x['create_time'] = x.pop('create_date')
                x['username'] = x.pop('user_name')
                x['db_type'] = cmdb.const.DB_ORACLE
                x.pop("database")
                m_oracle_cmdb = OracleCMDB(**x)
                m_session.add(m_oracle_cmdb)
                # TODO 任务也需要迁移过来
            for x in o_role_data_privilege:
                x = x.to_dict()
                x.pop('id')
                x['create_time'] = x.pop('create_date')
                m_role_cmdb_schema = RoleOracleCMDBSchema(**x)
                m_session.add(m_role_cmdb_schema)
            for x in o_data_health_user:
                x = x.to_dict()
                x.pop('needcalc')
                x['schema'] = x.pop('username')
                c_d = o_session.query(OCMDB). \
                    filter(OCMDB.connect_name == x.pop('database_name')). \
                    with_entities(OCMDB.cmdb_id).first()
                x['cmdb_id'] = c_d[0] if c_d else c_d
                m_rating_schema = OracleRatingSchema(**x)
                m_session.add(m_rating_schema)
            m_session.commit()
