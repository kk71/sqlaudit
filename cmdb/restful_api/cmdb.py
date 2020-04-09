# Author: kk.Fang(fkfkbill@gmail.com)

import operator
from sqlalchemy import or_
from functools import reduce
from collections import defaultdict

from cmdb.cmdb import CMDB
from cmdb.available_cmdb_schemas_utils import *
from cmdb.user_cmdb_utils import get_current_cmdb
from cmdb.score_utils import get_latest_cmdb_score
from cmdb.user_schema_utils import get_current_schema
from cmdb.test_cmdb_con_utils import test_cmdb_connectivity

from utils.const import *
from utils.schema_utils import *
from utils.product_license import *
from utils.conc_utils import async_thr
from utils.datetime_utils import d_to_str
from models.sqlalchemy import make_session, QueryEntity
from task.task import Task
from auth.user import *
from auth.restful_api.base import AuthReq,PrivilegeReq
from ticket.ticket import Ticket
from ticket.sub_ticket import SubTicket
from restful_api.modules import as_view


@as_view(group="cmdb")
class CMDBHandler(AuthReq):

    async def get(self):
        """查询纳管数据库列表"""
        params = self.get_query_args(Schema({
            # 只返回当前登录用户可见的cmdb
            scm_optional("current", default=not self.is_admin()): scm_bool,

            # 精确匹配
            scm_optional("cmdb_id"): scm_int,
            scm_optional("connect_name"): scm_unempty_str,
            scm_optional("group_name"): scm_unempty_str,
            scm_optional("business_name"): scm_unempty_str,

            # 模糊匹配多个字段
            scm_optional("keyword", default=None): scm_str,

            # 排序
            scm_optional("sort", default=SORT_DESC): And(scm_str, scm_one_of_choices(ALL_SORTS)),

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
            scm_optional("machine_room"): scm_int,
            "database_type": scm_int,
            "server_name": scm_str,
            "ip_address": scm_unempty_str,
            "port": scm_int,
            "service_name": scm_str,
            "user_name": scm_unempty_str,
            "password": scm_unempty_str,
            scm_optional("status", default=True): scm_bool,
            scm_optional("is_collect", default=True): scm_bool,
            scm_optional("auto_sql_optimized", default=True): scm_bool,
            scm_optional("domain_env"): scm_int,
            scm_optional("is_rac"): scm_bool,
            scm_optional("white_list_status"): scm_bool,
            scm_optional("while_list_rule_counts"): scm_int,
            "db_model": scm_unempty_str,
            "baseline": scm_int,
            "is_pdb": scm_bool,
            "version": scm_unempty_str,
            scm_optional("sid"): scm_str,
            scm_optional("allow_online", default=False): scm_bool
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

            scm_optional("ip_address"): scm_unempty_str,
            scm_optional("port"): scm_int,
            scm_optional("service_name"): scm_str,
            scm_optional("group_name"): scm_str,
            scm_optional("business_name"): scm_str,
            scm_optional("machine_room"): scm_str,
            scm_optional("database_type"): scm_unempty_str,
            scm_optional("server_name"): scm_str,
            scm_optional("user_name"): scm_unempty_str,
            scm_optional("password"): scm_unempty_str,
            scm_optional("is_collect"): scm_bool,
            scm_optional("status"): scm_bool,
            scm_optional("auto_sql_optimized"): scm_bool,
            scm_optional("domain_env"): scm_int,
            scm_optional("is_rac"): scm_bool,
            scm_optional("white_list_status"): scm_bool,
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


@as_view("aggregation", group="cmdb")
class CMDBAggregationHandler(PrivilegeReq):

    def get(self):
        """获取某个，某些字段的全部值的类型"""
        params = self.get_query_args(Schema({
            "key": And(
                scm_dot_split_str,
                scm_subset_of_choices(["connect_name", "group_name", "business_name"])
            )
        }))
        key = params.pop("key")
        with make_session() as session:
            ret = defaultdict(set)
            real_keys = [getattr(CMDB, k) for k in key]
            query_ret = session.query(CMDB).with_entities(*real_keys)
            if not self.is_admin():
                query_ret = query_ret.filter(
                    CMDB.cmdb_id.in_(get_current_cmdb(self.current_user)))
            for i, k in enumerate(key):
                for qr in query_ret:
                    ret[k].add(qr[i])
            ret = {k: list(v) for k, v in ret.items()}
            self.resp(ret)


@as_view("schema", group="cmdb")
class SchemaHandler(AuthReq):

    async def get(self):
        """获取schema列表"""

        DATA_PRIVILEGE = "data_privilege"
        HEALTH_USER_CONFIG = "health_user_config"

        params = self.get_query_args(Schema({
            scm_optional("cmdb_id", default=None): scm_int,
            scm_optional("connect_name", default=None): scm_unempty_str,
            scm_optional("current", default=not self.is_admin()): scm_bool,
            scm_optional("divide_by", default=None): scm_one_of_choices((
                DATA_PRIVILEGE,  # 以login_user区分当前库的数据权限（绑定、未绑定）
                HEALTH_USER_CONFIG  # 以login_user区分当前库的评分权限（绑定、未绑定）
            )),  # 指定分开返回的类型
            scm_optional("login_user", default=None): scm_str,
            scm_optional("role_id", default=None): scm_gt0_int,
        }))
        cmdb_id = params.pop("cmdb_id")
        connect_name = params.pop("connect_name")
        if not connect_name and not cmdb_id:
            return self.resp_bad_req(msg="neither cmdb_id nor connect_name is present.")
        current = params.pop("current")
        login_user = params.pop("login_user")
        role_id = params.pop("role_id")
        divide_by = params.pop("divide_by")
        del params
        with make_session() as session:
            if connect_name and not cmdb_id:
                cmdb = session.query(CMDB).filter_by(connect_name=connect_name).first()
                cmdb_id = cmdb.cmdb_id

            if login_user and divide_by == DATA_PRIVILEGE:
                # 返回给出的用户所绑定的schema，以及未绑定的
                bound = await async_thr(
                    get_current_schema, login_user, cmdb_id)
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif role_id and divide_by == DATA_PRIVILEGE:
                # 返回给出的角色所绑定的schema，以及未绑定的
                bound_schema_info = await async_thr(
                    get_current_schema, cmdb_id=cmdb_id, verbose=True)
                bound = list({schema_name for _, _, _, schema_name in bound_schema_info})
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif divide_by == HEALTH_USER_CONFIG:
                # 返回给出的库需要加入数据评分的schema，以及不需要的
                if connect_name:#TODO
                    bound = session.query(OracleRatingSchema.schema). \
                        filter(OracleRatingSchema.cmdb_id == cmdb_id)
                elif cmdb_id:
                    bound = session.query(OracleRatingSchema.schema). \
                        join(CMDB, OracleRatingSchema.cmdb_id == CMDB.cmdb_id). \
                        filter(CMDB.cmdb_id == cmdb_id)
                else:
                    assert 0
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                bound = [i[0] for i in bound]
                try:
                    all_schemas = await async_thr(
                        get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif current:
                # 当前登录用户可用(数据权限配置)的schema
                current_schemas = await async_thr(
                    get_current_schema, self.current_user, cmdb_id)
                self.resp(current_schemas)

            else:
                # 当前cmdb的全部的schema，不考虑数据权限
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp(all_schemas)


@as_view("score_trend", group="cmdb")
class CMDBHealthTrendHandler(AuthReq):

    def post(self):
        """健康评分趋势图"""
        params = self.get_json_args(Schema({
            scm_optional("cmdb_id_list", default=()): list
        }), default_body="{}")
        now = arrow.now()
        cmdb_id_list = params.pop("cmdb_id_list")

        with make_session() as session:
            if not cmdb_id_list:
                cmdb_id_list = get_current_cmdb(user_login=self.current_user)
                # 如果没有给出cmdb_id，则把最差的前十个拿出来
                from utils.cmdb_utils import get_la
                cmdb_id_list = [
                                   i
                                   for i in cmdb_utils.get_latest_cmdb_score(session=session).keys()
                                   if i in cmdb_id_list
                               ][:10]
            fields = set()
            ret = defaultdict(dict)  # {date: [{health data}, ...]}
            for cmdb_id in cmdb_id_list:
                dh_q = StatsCMDBRate.objects(
                    cmdb_id=cmdb_id,
                    etl_date__gt=now.shift(weeks=-2).datetime
                ).order_by("etl_date")
                for dh in dh_q:
                    ret[dh.etl_date.date()][dh.connect_name] = dh.score
                    fields.add(dh.connect_name)
            base_lines = [
                i[0]
                for i in session.
                    query(CMDB.baseline).
                    filter(CMDB.cmdb_id.in_(cmdb_id_list)).
                    order_by(CMDB.baseline)
            ]
            if not base_lines or base_lines[0] == 0:
                base_line = 80
            else:
                base_line = base_lines[0]
            ret = [{
                "date": d_to_str(k),
                **v
            } for k, v in ret.items()]
            self.resp({
                "data": ret,
                "fields": list(fields),
                "base_line": base_line
            })

