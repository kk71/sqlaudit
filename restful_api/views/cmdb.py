# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from collections import defaultdict
from functools import reduce

from schema import Schema, Optional, And
from sqlalchemy import or_

from utils.schema_utils import *
from utils.perf_utils import *
from utils.const import *
from utils.datetime_utils import *
from utils.cmdb_utils import get_cmdb_available_schemas
from .base import *
from utils import cmdb_utils
from models.oracle import *
from models.mongo import *
from task.clear_cache import clear_cache
from utils.conc_utils import async_thr


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
            all_db_data_health = cmdb_utils.get_latest_cmdb_score(session).values()
            if current:
                current_cmdb_ids: list = await async_thr(
                    cmdb_utils.get_current_cmdb, session, self.current_user)
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
                                 RoleDataPrivilege.schema_name,
                                 RoleDataPrivilege.create_date,
                                 RoleDataPrivilege.comments,
                                 Role.role_name,
                                 Role.role_id)
                cmdb_role = session.query(*qe). \
                    join(CMDB, RoleDataPrivilege.cmdb_id == CMDB.cmdb_id). \
                    join(Role, Role.role_id == RoleDataPrivilege.role_id)

                cmdb_role = [list(x) for x in cmdb_role]
                cmdb_role = [x for x in cmdb_role if i['cmdb_id'] in x]

                i['role'] = reduce(lambda x, y: x if y in x else x + [y],
                                   [[], ] + [{'role_id': a[6], 'role_name': a[5]} for a in cmdb_role])

                keys = QueryEntity(
                    UserRole.role_id,
                    Role.role_name,
                    User.login_user,
                    User.user_name
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
                new_task = TaskManage(
                    task_exec_scripts=task_type,
                    **task_dict
                )
                session.add(new_task)
            session.commit()
            session.refresh(new_cmdb)
            clear_cache.delay()
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

            if "ip_address" in params.keys() and\
                    "port" in params.keys() and\
                    "service_name" in params.keys():
                qqq = session.query(CMDB).filter(
                        CMDB.ip_address == params["ip_address"],
                        CMDB.port == params["port"],
                        or_(  # TODO 记得改，目前sid和sid的字段名和实际意义是反过来的
                            CMDB.service_name == params["service_name"],
                            # CMDB.sid == params["sid"]
                        )
                )
                if not qqq.filter(CMDB.cmdb_id == cmdb_id).count() and qqq.count():
                    return self.resp_bad_req(msg="IP地址-端口-service_name与已有的纳管库重复。")

            the_cmdb.from_dict(params)

            # 同步更新全部任务的数据库字段信息
            session.query(TaskManage).filter_by(cmdb_id=the_cmdb.cmdb_id).update(
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
                session.query(TaskManage).filter_by(cmdb_id=the_cmdb.cmdb_id). \
                    update({"task_status": True})
            else:
                session.query(TaskManage).filter_by(cmdb_id=the_cmdb.cmdb_id). \
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
            session.query(TaskManage).filter_by(**params).delete(synchronize_session=False)
            session.query(RoleDataPrivilege).filter_by(**params).delete(synchronize_session=False)
            session.query(WorkList).filter_by(**params).delete(synchronize_session=False)
            TicketSubResult.objects(**params).delete()
        clear_cache.delay()
        self.resp_created(msg="已删除。")

    async def options(self):
        """测试连接是否成功"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int
        }))
        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(**params).first()
            resp = await async_thr(cmdb_utils.test_cmdb_connectivity, cmdb)
            self.resp(resp)


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
                    CMDB.cmdb_id.in_(cmdb_utils.get_current_cmdb(session, self.current_user)))
            for i, k in enumerate(key):
                for qr in query_ret:
                    ret[k].add(qr[i])
            ret = {k: list(v) for k, v in ret.items()}
            self.resp(ret)


class SchemaHandler(AuthReq):

    async def get(self):
        """获取schema列表"""

        DATA_PRIVILEGE = "data_privilege"
        HEALTH_USER_CONFIG = "health_user_config"

        params = self.get_query_args(Schema({
            Optional("cmdb_id", default=None): scm_int,
            Optional("connect_name", default=None): scm_unempty_str,
            Optional("current", default=not self.is_admin()): scm_bool,
            Optional("divide_by", default=None): scm_one_of_choices((
                DATA_PRIVILEGE,  # 以login_user区分当前库的数据权限（绑定、未绑定）
                HEALTH_USER_CONFIG  # 以login_user区分当前库的评分权限（绑定、未绑定）
            )),  # 指定分开返回的类型
            Optional("login_user", default=None): scm_str,
            Optional("role_id", default=None): scm_gt0_int,
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
                    cmdb_utils.get_current_schema, session, login_user, cmdb_id)
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        cmdb_utils.get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif role_id and divide_by == DATA_PRIVILEGE:
                # 返回给出的角色所绑定的schema，以及未绑定的
                bound_schema_info = await async_thr(
                    cmdb_utils.get_current_schema, session, cmdb_id=cmdb_id, verbose=True)
                bound = list({schema_name for _, _, _, schema_name in bound_schema_info})
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        cmdb_utils.get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif divide_by == HEALTH_USER_CONFIG:
                # 返回给出的库需要加入数据评分的schema，以及不需要的
                if connect_name:
                    bound = session.query(DataHealthUserConfig.username). \
                        filter(DataHealthUserConfig.database_name == connect_name)
                elif cmdb_id:
                    bound = session.query(DataHealthUserConfig.username). \
                        join(CMDB, DataHealthUserConfig.database_name == CMDB.connect_name). \
                        filter(CMDB.cmdb_id == cmdb_id)
                else:
                    assert 0
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                bound = [i[0] for i in bound]
                try:
                    all_schemas = await async_thr(
                        cmdb_utils.get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif current:
                # 当前登录用户可用(数据权限配置)的schema
                current_schemas = await async_thr(
                    cmdb_utils.get_current_schema, session, self.current_user, cmdb_id)
                self.resp(current_schemas)

            else:
                # 当前cmdb的全部的schema，不考虑数据权限
                cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        cmdb_utils.get_cmdb_available_schemas, cmdb)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                self.resp(all_schemas)


class CMDBHealthTrendHandler(AuthReq):

    @timing()
    def post(self):
        """健康评分趋势图"""
        params = self.get_json_args(Schema({
            Optional("cmdb_id_list", default=()): list
        }), default_body="{}")
        now = arrow.now()
        cmdb_id_list = params.pop("cmdb_id_list")

        with make_session() as session:
            if not cmdb_id_list:
                cmdb_id_list = cmdb_utils.get_current_cmdb(
                    session, user_login=self.current_user)
                # 如果没有给出cmdb_id，则把最差的前十个拿出来
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


class RankingConfigHandler(AuthReq):

    def get(self):
        """获取需要评分的数据库列表"""
        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")
        del params

        with make_session() as session:
            qe = QueryEntity(
                DataHealthUserConfig.database_name,
                CMDB.cmdb_id,
                DataHealthUserConfig.username,
                DataHealthUserConfig.needcalc,
                DataHealthUserConfig.weight
            )
            rankings = session. \
                query(*qe). \
                join(CMDB, DataHealthUserConfig.database_name == CMDB.connect_name)
            if keyword:
                rankings = self.query_keyword(rankings, keyword,
                                              CMDB.connect_name,
                                              DataHealthUserConfig.username)
            items, p = self.paginate(rankings, **p)
            self.resp(sorted([qe.to_dict(i) for i in items], key=lambda k: k["database_name"]), **p)

    def post(self):
        """以库为单位修改(增删)评分权重"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "schema_names": [scm_unempty_str]
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_names = set(params.pop("schema_names"))
        del params

        with make_session() as session:
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).first()
            try:
                schemas = get_cmdb_available_schemas(cmdb)
            except cx_Oracle.DatabaseError as err:
                print(err)
                return self.resp_bad_req(msg="无法连接到目标主机")
            schema_delta_not_existed = schema_names - set(schemas)
            if schema_delta_not_existed:
                print(schemas)
                return self.resp_bad_req(
                    msg="给出的schema中包含该库不存在的schema："
                        f"{', '.join([i for i in schema_delta_not_existed])}")
            qe = QueryEntity(DataHealthUserConfig.username)
            schema_names_current = qe.to_plain_list(
                session.query(*qe).filter(
                    DataHealthUserConfig.database_name == cmdb.connect_name))
            schema_names_current = set(schema_names_current)
            schema_names_to_delete = schema_names_current.difference(schema_names)
            schema_names_to_add = schema_names.difference(schema_names_current)
            session.query(DataHealthUserConfig).filter(
                DataHealthUserConfig.database_name == cmdb.connect_name,
                DataHealthUserConfig.username.in_(list(schema_names_to_delete))
            ).delete(synchronize_session='fetch')
            session.add_all([DataHealthUserConfig(
                database_name=cmdb.connect_name,
                username=i,
                weight=1.0
            ) for i in schema_names_to_add])
        self.resp_created(msg="评分配置成功")

    def patch(self):
        """以cmdb-schema为单位修改评分权重"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "username": scm_unempty_str,
            "weight": self.scm_with_em(And(scm_float, lambda x: x <= 1), e="权重不可大于1")
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("username")

        with make_session() as session:
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).first()
            session.query(DataHealthUserConfig).filter_by(
                database_name=cmdb.connect_name,
                username=schema_name,
            ).update(params)
        self.resp_created(msg="评分配置更新成功")

    def delete(self):
        """删除需要评分的库"""
        params = self.get_json_args(Schema({
            'database_name': scm_unempty_str,
            'username': scm_unempty_str
        }))
        with make_session() as session:
            session.query(DataHealthUserConfig). \
                filter_by(**params).delete(synchronize_session=False)
        self.resp_created("删除评分schema成功")
