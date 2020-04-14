# Author: kk.Fang(fkfkbill@gmail.com)

# old oracle data
from utils.migrate_from_oracle import make_oracle_session
from utils.migrate_from_oracle import User as OUser, UserRole as OUserRole, \
    RolePrivilege as ORolePrivilege, Role as ORole, CMDB as OCMDB, \
    RoleDataPrivilege as ORoleDataPrivilege, \
    DataHealthUserConfig as ODataHealthUserConfig, \
    TaskManage as OTaskManage

# new mysql data
from models import init_models

init_models()

import cmdb.const
import task.const
from models.sqlalchemy import make_session
from auth.user import *
from oracle_cmdb.cmdb import *
from oracle_cmdb.rate import *
from oracle_cmdb.auth.role import *
from cmdb.cmdb_task import *


def main():
    """ONLY RUN ONCE!! oracle data migrate to mysql"""

    with make_oracle_session() as o_session:
        o_user_q = o_session.query(OUser)
        o_user_role_q = o_session.query(OUserRole)
        o_role_privilege = o_session.query(ORolePrivilege)
        o_role = o_session.query(ORole)
        o_cmdb = o_session.query(OCMDB)
        o_task_manage = o_session.query(OTaskManage).filter_by(
            task_exec_scripts="采集及分析")
        o_role_data_privilege = o_session.query(ORoleDataPrivilege)
        o_data_health_user = o_session.query(
            OCMDB.cmdb_id,
            ODataHealthUserConfig.username,
            ODataHealthUserConfig.weight
        ).filter(OCMDB.connect_name == ODataHealthUserConfig.database_name)

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
                x.pop("database_type")
                # 老代码service_name和sid是互换的
                x["sid"], x["service_name"] = x["service_name"], x["sid"]
                m_oracle_cmdb = OracleCMDB(**x)
                m_session.add(m_oracle_cmdb)
            for x in o_task_manage:
                x = x.to_dict()
                x['db_type'] = cmdb.const.DB_ORACLE
                x.pop('database_type')
                x['schedule_time'] = x.pop('task_schedule_date')
                x['id'] = x.pop('task_id')
                x['status'] = x.pop('task_status')
                x['frequency'] = x.pop('task_exec_frequency')
                x.pop('port')
                x.pop('business_name')
                x.pop('ip_address')
                x.pop('task_exec_scripts')
                x.pop('server_name')
                x.pop('machine_room')
                x.pop('task_create_date')
                x.pop("last_task_exec_succ_date")
                x.pop("task_exec_counts")
                x.pop("task_exec_success_count")
                m_task = CMDBTask(
                    task_type=task.const.TASK_TYPE_CAPTURE,
                    task_name=task.const.ALL_TASK_TYPE_CHINESE[
                                  task.const.TASK_TYPE_CAPTURE],
                    **x
                )
                m_session.add(m_task)
            for x in o_role_data_privilege:
                x = x.to_dict()
                x.pop('id')
                x['create_time'] = x.pop('create_date')
                m_role_cmdb_schema = RoleOracleCMDBSchema(**x)
                m_session.add(m_role_cmdb_schema)
            for the_cmdb_id, the_schema, the_weight in o_data_health_user:
                m_session.add(OracleRatingSchema(
                    cmdb_id=the_cmdb_id,
                    schema=the_schema,
                    weight=the_weight
                ))
            m_session.commit()
            print("done")
