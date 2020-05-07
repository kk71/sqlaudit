
__all__ = [
    "BaseCMDBHandler"
]

from sqlalchemy import or_
from functools import reduce
from collections import defaultdict

from ..const import *
from cmdb.cmdb import *
from cmdb.cmdb_task import CMDBTask
from auth.user import *
from auth.restful_api.base import *
from auth.product_license import *
from oracle_cmdb.cmdb import OracleCMDB
from oracle_cmdb.cmdb_utils import get_latest_cmdb_score
from utils.schema_utils import *
from utils.conc_utils import async_thr
from models.sqlalchemy import *
from ticket.ticket import *
from ticket.sub_ticket import SubTicket
from restful_api.modules import as_view
from oracle_cmdb.auth.user_utils import current_cmdb
from oracle_cmdb.auth.role import RoleOracleCMDBSchema

@as_view(group="cmdb")
class BaseCMDBHandler(AuthReq):

    def gen_current(self):
        """是否仅返回当前登录用户纳管的库,如果是admin则返回所有"""
        return {
            scm_optional("current", default=not self.is_admin()): scm_bool
        }

    def get_queryset(self, session):
        params = self.get_query_args(Schema({
            # 精确匹配
            scm_optional("cmdb_id"): scm_gt0_int,
            scm_optional("connect_name"): scm_unempty_str,
            scm_optional("group_name"): scm_unempty_str,
            scm_optional("business_name"): scm_unempty_str,

            # 模糊匹配多个字段
            scm_optional("keyword", default=None): scm_str,

            # 只返回当前登录用户可见的cmdb
            ** self.gen_current(),

            # 排序
            scm_optional("sort", default=SORT_DESC): And(
                scm_str, self.scm_one_of_choices(ALL_SORTS)),

            ** self.gen_p()
        }, ignore_extra_keys=True))
        keyword = params.pop("keyword")
        current = params.pop("current")
        sort = params.pop("sort")
        paging = self.pop_p(params)

        cmdb_q = session.query(CMDB).filter_by(**params)
        if keyword:
            cmdb_q = self.query_keyword(cmdb_q, keyword,
                                    CMDB.cmdb_id,
                                    CMDB.connect_name,
                                    CMDB.group_name,
                                    CMDB.business_name,
                                    CMDB.server_name,
                                    CMDB.ip_address)
        return cmdb_q,paging,current,sort

    async def get(self):
        """查询cmdb列表"""
        with make_session() as session:
            cmdb_q,paging,current,sort=self.get_queryset(session)
            # 获取纳管库的评分
            all_db_data_health = get_latest_cmdb_score(session).values()
            if current:
                current_cmdb_ids: list = await async_thr(
                    current_cmdb, session, self.current_user)
                cmdb_q = cmdb_q.filter(CMDB.cmdb_id.in_(current_cmdb_ids))
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
            all_current_cmdb = {cmdb.cmdb_id: cmdb for cmdb in cmdb_q}
            for data_health in all_db_data_health:
                cmdb_obj_of_this_dh = all_current_cmdb.get(data_health.cmdb_id)
                if not cmdb_obj_of_this_dh:
                    print(f"{data_health.cmdb_id} not found")
                    continue
                ret.append({
                    **cmdb_obj_of_this_dh.to_dict(),
                    "data_health": data_health.to_dict()
                })
            ret, p = self.paginate(ret, **paging)

            # 对分页之后的纳管库列表补充额外数据
            #TODO stats
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
                                 RoleOracleCMDBSchema.schema_name,
                                 RoleOracleCMDBSchema.create_time,
                                 RoleOracleCMDBSchema.comments,
                                 Role.role_name,
                                 Role.role_id)
                cmdb_role = session.query(*qe). \
                    join(CMDB, RoleOracleCMDBSchema.cmdb_id == CMDB.cmdb_id). \
                    join(Role, Role.role_id == RoleOracleCMDBSchema.role_id)

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
            "db_type": scm_str,
            "server_name": scm_str,
            "ip_address": scm_unempty_str,
            "port": scm_int,
            "service_name": scm_str,
            "username": scm_unempty_str,
            "password": scm_unempty_str,
            scm_optional("status", default=True): scm_bool,
            scm_optional("domain_env"): scm_int,
            scm_optional("is_rac"): scm_bool,
            "db_model": scm_unempty_str,
            "baseline": scm_int,
            "is_pdb": scm_bool,
            "version": scm_unempty_str,
            scm_optional("sid"): scm_str,
            scm_optional("allow_online", default=False): scm_bool
        }))
        with make_session() as session:

            cmdb_count = len(session.query(CMDB).all())
            license_key = SqlAuditLicenseKeyManager.latest_license_key()
            license_key_ins = SqlAuditLicenseKey.decode(license_key)
            if cmdb_count:
                if cmdb_count >= license_key_ins.database_counts:
                    return self.resp_forbidden(msg="纳管库数量已到上线")

            new_cmdb = OracleCMDB(**params)

            # 检测数据库是否有重复信息
            if session.query(OracleCMDB).filter_by(connect_name=params["connect_name"]).first():
                return self.resp_bad_req(msg="连接名称已存在")

            if session.query(OracleCMDB).filter(
                    OracleCMDB.ip_address == params["ip_address"],
                    OracleCMDB.port == params["port"],
                    or_(  # TODO 记得改，目前sid和service_name的字段名和实际意义是反过来的
                        OracleCMDB.service_name == params["service_name"],
                        OracleCMDB.sid == params["sid"]
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
                    "cmdb_id",
                    "group_name",
                    "db_type",
                ))
                new_task = CMDBTask(task_type=task_type, **task_dict)
                session.add(new_task)
            session.commit()
            session.refresh(new_cmdb)
            self.resp_created(new_cmdb.to_dict())

    def patch(self):
        """修改CMDB"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_unempty_str,

            scm_optional("ip_address"): scm_unempty_str,
            scm_optional("port"): scm_int,
            scm_optional("service_name"): scm_str,
            scm_optional("group_name"): scm_str,
            scm_optional("business_name"): scm_str,
            scm_optional("db_type"): scm_unempty_str,
            scm_optional("server_name"): scm_str,
            scm_optional("username"): scm_unempty_str,
            scm_optional("password"): scm_unempty_str,
            scm_optional("status"): scm_bool,
            scm_optional("domain_env"): scm_int,
            scm_optional("is_rac"): scm_bool,
            scm_optional("db_model"): scm_unempty_str,
            scm_optional("baseline"): scm_int,
            scm_optional("is_pdb"): scm_bool,
            scm_optional("version"): scm_unempty_str,
            scm_optional("sid"): scm_str,
            scm_optional("allow_online"): scm_bool  # 这个字段只有admin可以修改
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
                        CMDB.sid == params["sid"]
                    )
            ).first():
                return self.resp_bad_req(msg="IP地址-端口-service_name与已有的纳管库重复。")

            the_cmdb.from_dict(params)

            # 同步更新全部任务的数据库字段信息
            session.query(CMDBTask).filter_by(cmdb_id=the_cmdb.cmdb_id).update(
                the_cmdb.to_dict(iter_if=lambda k, v: k in (
                    "connect_name",
                    "group_name",
                    "db_type",
                ))
            )

            # 更新采集开关
            if the_cmdb.is_collect:
                session.query(CMDBTask).filter_by(cmdb_id=the_cmdb.cmdb_id). \
                    update({"task_status": True})
            else:
                session.query(CMDBTask).filter_by(cmdb_id=the_cmdb.cmdb_id). \
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
            session.query(CMDBTask).filter_by(**params).delete(synchronize_session=False)
            session.query(RoleOracleCMDBSchema).filter_by(**params).delete(synchronize_session=False)
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
            resp = await async_thr(cmdb.test_cmdb_connectivity)#TODO
            self.resp(resp)

@as_view("cmdb_aggregation",group="cmdb")
class CMDBAggregationHandler(PrivilegeReq):

    def get(self):
        """获取cmdb某个或某些字段的聚合值"""
        params = self.get_query_args(Schema({
            "key": And(
                scm_dot_split_str,
                self.scm_subset_of_choices([
                    "connect_name", "group_name", "business_name"])
            )
        }))
        key = params.pop("key")
        with make_session() as session:
            ret = defaultdict(set)
            real_keys = [getattr(CMDB, k) for k in key]
            query_ret = session.query(CMDB).with_entities(*real_keys)
            if not self.is_admin():
                query_ret = query_ret.filter(
                    CMDB.cmdb_id.in_(current_cmdb(session,self.current_user)))
            for i, k in enumerate(key):
                for qr in query_ret:
                    ret[k].add(qr[i])
            ret = {k: list(v) for k, v in ret.items()}
            self.resp(ret)
