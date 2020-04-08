# Author: kk.Fang(fkfkbill@gmail.com)

import operator
from sqlalchemy import or_
from functools import reduce
from schema import Schema, Optional

from cmdb.cmdb import CMDB
from cmdb.user_cmdb_utils import get_current_cmdb
from cmdb.score_utils import get_latest_cmdb_score
from cmdb.test_cmdb_con_utils import test_cmdb_connectivity

from utils.const import *
from utils.schema_utils import *
from utils.product_license import *
from utils.conc_utils import async_thr
from models.sqlalchemy import make_session, QueryEntity
from task.task import Task
from auth.user import *
from auth.restful_api.base import AuthReq
from ticket.ticket import Ticket
from ticket.sub_ticket import SubTicket
from oracle_cmdb.cmdb import RoleCMDBSchema


class CMDBHandler(AuthReq):

    async def get(self):
        """查询纳管数据库列表"""
        params = self.get_query_args(Schema({
            # 只返回当前登录用户可见的cmdb
            Optional("current", default=not self.is_admin()): scm_bool,

            # 精确匹配
            Optional("cmdb_id"): scm_int,
            Optional("connect_name"): scm_unempty_str,
            Optional("group_name"): scm_unempty_str,
            Optional("business_name"): scm_unempty_str,

            # 模糊匹配多个字段
            Optional("keyword", default=None): scm_str,

            # 排序
            Optional("sort", default=SORT_DESC): And(scm_str, scm_one_of_choices(ALL_SORTS)),

            **self.gen_p(),
        }))
        keyword = params.pop("keyword")
        current = params.pop("current")
        sort = params.pop("sort")
        p = self.pop_p(params)

        with make_session() as session:
            q = session.query(CMDB).filter_by(**params)
            if keyword:
                q = self.query_keyword(q, keyword,
                                       CMDB.cmdb_id,
                                       CMDB.connect_name,
                                       CMDB.group_name,
                                       CMDB.business_name,
                                       CMDB.server_name,
                                       CMDB.ip_address
                                       )
            # 获取纳管库的评分
            all_db_data_health = get_latest_cmdb_score().values()
            if current:
                current_cmdb_ids: list = await async_thr(
                    get_current_cmdb, self.current_user)
                q = q.filter(CMDB.cmdb_id.in_(current_cmdb_ids))
                all_db_data_health = [
                    stats_cmdb_rate
                    for stats_cmdb_rate in all_db_data_health
                    if stats_cmdb_rate.cmdb_id in current_cmdb_ids
                ]
            if all_db_data_health:
                all_db_data_health = sorted(
                    all_db_data_health,
                    key=lambda da: da.score if da.score is not None else 0,
                    reverse=True)
                if sort == SORT_DESC:
                    pass
                elif sort == SORT_ASC:
                    all_db_data_health.reverse()
                else:
                    assert 0

            # 构建输出的纳管库信息（分页前）
            ret = []
            all_current_cmdb = {cmdb.cmdb_id: cmdb for cmdb in q}
            for data_health in all_db_data_health:
                cmdb_obj_of_this_dh = all_current_cmdb.get(data_health.cmdb_id)
                if not cmdb_obj_of_this_dh:
                    print(f"{data_health.cmdb_id} not found")
                    continue
                ret.append({
                    **cmdb_obj_of_this_dh.to_dict(),
                    "data_health": data_health.to_dict()
                })
            ret, p = self.paginate(ret, **p)

            # 对分页之后的纳管库列表补充额外数据
            login_stats = StatsLoginUser.objects(login_user=self.current_user). \
                order_by("-etl_date").first()
            if login_stats:
                login_stats = login_stats.to_dict()
                cmdb_stats = {c["cmdb_id"]: c for c in login_stats["cmdb"]}
                the_etl_date = login_stats["etl_date"]
            else:
                cmdb_stats = {}
                the_etl_date = None
            for i in ret:
                i["stats"] = cmdb_stats.get(i["cmdb_id"], {})
                i["stats"]["etl_date"] = the_etl_date
                # TODO 这里给ret加上纳管它的角色信息（角色名，角色id）
                #  以及纳管它的用户(login_user, user_name)
                qe = QueryEntity(CMDB.connect_name,
                                 CMDB.cmdb_id,
                                 RoleCMDBSchema.schema_name,
                                 RoleCMDBSchema.create_time,
                                 RoleCMDBSchema.comments,
                                 Role.role_name,
                                 Role.role_id)
                cmdb_role = session.query(*qe). \
                    join(CMDB, RoleCMDBSchema.cmdb_id == CMDB.cmdb_id). \
                    join(Role, Role.role_id == RoleCMDBSchema.role_id)

                cmdb_role = [list(x) for x in cmdb_role]
                cmdb_role = [x for x in cmdb_role if i['cmdb_id'] in x]

                i['role'] = reduce(lambda x, y: x if y in x else x + [y],
                                   [[], ] + [{'role_id': a[6], 'role_name': a[5]} for a in cmdb_role])

                keys = QueryEntity(
                    UserRole.role_id,
                    Role.role_name,
                    User.login_user,
                    User.username
                )
                role_user = session.query(*keys). \
                    join(Role, UserRole.role_id == Role.role_id). \
                    join(User, UserRole.login_user == User.login_user)

                role_user = [list(x) for x in role_user]
                role_user = [y for y in role_user for d in i['role'] if d['role_id'] in y]

                i['combined_user'] = reduce(
                    lambda x, y: x if y in x else x + [y],
                    [[]] + [
                        {'combined_login_user': c[2], 'combined_user_name': c[3]}
                        for c in role_user
                    ]
                )

            self.resp(ret, **p)

    def post(self):
        """增加CMDB"""
        params = self.get_json_args(Schema({
            "connect_name": scm_unempty_str,
            "group_name": scm_str,
            "business_name": scm_str,
            Optional("machine_room"): scm_int,
            "database_type": scm_int,
            "server_name": scm_str,
            "ip_address": scm_unempty_str,
            "port": scm_int,
            "service_name": scm_str,
            "user_name": scm_unempty_str,
            "password": scm_unempty_str,
            Optional("status", default=True): scm_bool,
            Optional("is_collect", default=True): scm_bool,
            Optional("auto_sql_optimized", default=True): scm_bool,
            Optional("domain_env"): scm_int,
            Optional("is_rac"): scm_bool,
            Optional("white_list_status"): scm_bool,
            Optional("while_list_rule_counts"): scm_int,
            "db_model": scm_unempty_str,
            "baseline": scm_int,
            "is_pdb": scm_bool,
            "version": scm_unempty_str,
            Optional("sid"): scm_str,
            Optional("allow_online", default=False): scm_bool
        }))
        params["create_owner"] = self.current_user
        with make_session() as session:
            cmdb_count=len(session.query(CMDB).all())
            license_key = SqlAuditLicenseKeyManager.latest_license_key()
            license_key_ins = SqlAuditLicenseKey.decode(license_key)
            if cmdb_count:
                if operator.ge(cmdb_count, license_key_ins.database_counts):
                    return self.resp_forbidden(msg="纳管库数量已到上线")

            new_cmdb = CMDB(**params)

            # 检测数据库是否有重复信息
            if session.query(CMDB).filter_by(connect_name=params["connect_name"]).first():
                return self.resp_bad_req(msg="连接名称已存在")

            if session.query(CMDB).filter(
                    CMDB.ip_address == params["ip_address"],
                    CMDB.port == params["port"],
                    or_(  # TODO 记得改，目前sid和service_name的字段名和实际意义是反过来的
                        CMDB.service_name == params["service_name"],
                        # CMDB.sid == params["sid"]
                    )
            ).first():
                return self.resp_bad_req(msg="IP地址-端口-sid与已有的纳管库重复。")

            session.add(new_cmdb)
            session.commit()
            session.refresh(new_cmdb)

            # 创建任务的数据库字段信息
            for task_type in ALL_DB_TASKS:
                task_dict = new_cmdb.to_dict(iter_if=lambda k, v: k in (
                    "connect_name",
                    "group_name",
                    "business_name",
                    "machine_room",
                    "database_type",
                    "server_name",
                    "ip_address",
                    "port",
                    "cmdb_id"
                ))

                new_task = Task(task_exec_scripts=task_type,**task_dict)
                session.add(new_task)
            session.commit()
            session.refresh(new_cmdb)
            self.resp_created(new_cmdb.to_dict())

    def patch(self):
        """修改CMDB"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_unempty_str,

            Optional("ip_address"): scm_unempty_str,
            Optional("port"): scm_int,
            Optional("service_name"): scm_str,
            Optional("group_name"): scm_str,
            Optional("business_name"): scm_str,
            Optional("machine_room"): scm_str,
            Optional("database_type"): scm_unempty_str,
            Optional("server_name"): scm_str,
            Optional("user_name"): scm_unempty_str,
            Optional("password"): scm_unempty_str,
            Optional("is_collect"): scm_bool,
            Optional("status"): scm_bool,
            Optional("auto_sql_optimized"): scm_bool,
            Optional("domain_env"): scm_int,
            Optional("is_rac"): scm_bool,
            Optional("white_list_status"): scm_bool,
            Optional("db_model"): scm_unempty_str,
            Optional("baseline"): scm_int,
            Optional("is_pdb"): scm_bool,
            Optional("version"): scm_unempty_str,
            Optional("sid"): scm_str,
            Optional("allow_online"): scm_bool  # 这个字段只有admin可以修改
        }))
        cmdb_id = params.pop("cmdb_id")

        with make_session() as session:
            the_cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()

            if not self.is_admin() \
                    and "allow_online" in params.keys() \
                    and params["allow_online"] != the_cmdb.allow_online:
                return self.resp_forbidden("只有管理员可以操作自助上线开关")

            if session.query(CMDB).filter(
                    CMDB.ip_address == params["ip_address"],
                    CMDB.port == params["port"],
                    or_(  # TODO 记得改，目前sid和sid的字段名和实际意义是反过来的
                        CMDB.service_name == params["service_name"],
                        # CMDB.sid == params["sid"]
                    )
            ).first():
                return self.resp_bad_req(msg="IP地址-端口-service_name与已有的纳管库重复。")

            the_cmdb.from_dict(params)

            # 同步更新全部任务的数据库字段信息
            session.query(Task).filter_by(cmdb_id=the_cmdb.cmdb_id).update(
                the_cmdb.to_dict(iter_if=lambda k, v: k in (
                    "connect_name",
                    "group_name",
                    "business_name",
                    "machine_room",
                    "database_type",
                    "server_name",
                    "ip_address",
                    "port"
                ))
            )

            # 更新采集开关
            if the_cmdb.is_collect:
                session.query(Task).filter_by(cmdb_id=the_cmdb.cmdb_id). \
                    update({"task_status": True})
            else:
                session.query(Task).filter_by(cmdb_id=the_cmdb.cmdb_id). \
                    update({"task_status": False})

            session.add(the_cmdb)
            session.commit()
            session.refresh(the_cmdb)
            self.resp_created(the_cmdb.to_dict())

    def delete(self):
        """删除CMDB"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_unempty_str,
        }))
        with make_session() as session:
            the_cmdb = session.query(CMDB).filter_by(**params).first()
            session.delete(the_cmdb)
            session.query(Task).filter_by(**params).delete(synchronize_session=False)
            session.query(RoleCMDBSchema).filter_by(**params).delete(synchronize_session=False)
            Ticket.objects().filter_by(**params).delete()
            SubTicket.objects(**params).delete()
        self.resp_created(msg="已删除。")

    async def options(self):
        """测试连接是否成功"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int
        }))
        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(**params).first()
            resp = await async_thr(test_cmdb_connectivity, cmdb)
            self.resp(resp)


