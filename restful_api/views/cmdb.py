# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from collections import defaultdict

from schema import Schema, Optional, And

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
            Optional("current", default=not self.is_admin()): scm_bool,  # 只返回当前登录用户可见的cmdb

            # 精确匹配
            Optional("cmdb_id"): scm_int,
            Optional("connect_name"): scm_unempty_str,
            Optional("group_name"): scm_unempty_str,
            Optional("business_name"): scm_unempty_str,

            # 模糊匹配多个字段
            Optional("keyword", default=None): scm_str,

            # 分页
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,

            # 排序
            Optional("sort", default=SORT_DESC): And(scm_str, scm_one_of_choices(ALL_SORTS))
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
                                       CMDB.machine_room,
                                       CMDB.server_name,
                                       CMDB.ip_address
                                       )
            if current:
                current_cmdb_ids: list = await async_thr(
                    cmdb_utils.get_current_cmdb, session, self.current_user)
                q = q.filter(CMDB.cmdb_id.in_(current_cmdb_ids))
                all_db_data_health = await async_thr(
                    cmdb_utils.get_latest_health_score_cmdb, session, self.current_user)
                if all_db_data_health:
                    if sort == SORT_DESC:
                        all_db_data_health = sorted(all_db_data_health, key=lambda da: da['health_score']
                        if da['health_score'] is not None else 0,
                                                    reverse=True)
                    elif sort == SORT_ASC:
                        all_db_data_health = sorted(all_db_data_health, key=lambda da: da['health_score']
                        if da['health_score'] is not None else 0,
                                                    reverse=False)
            else:
                all_db_data_health = await async_thr(
                    cmdb_utils.get_latest_health_score_cmdb, session)
            ret = []
            if "cmdb_id" in params.keys():
                p = {}
                cmdb_dict = q.first().to_dict()
                for data_health in all_db_data_health:
                    if data_health["collect_date"]:
                        data_health["collect_date"] = d_to_str(data_health["collect_date"])
                    if data_health["connect_name"] == cmdb_dict["connect_name"]:
                        ret.append({
                            **cmdb_dict,
                            "data_health": data_health
                        })
                        break
            else:
                for data_health in all_db_data_health:
                    cmdb_obj_of_this_dh = q. \
                        filter(CMDB.connect_name == data_health["connect_name"]). \
                        first()
                    if not cmdb_obj_of_this_dh:
                        continue
                    if data_health["collect_date"]:
                        data_health["collect_date"] = d_to_str(data_health["collect_date"])
                    ret.append({
                        **cmdb_obj_of_this_dh.to_dict(),
                        "data_health": data_health
                    })
                ret, p = self.paginate(ret, **p)
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
            #TODO 这里给ret加上纳管它的角色信息（角色名，角色id）以及纳管它的用户(login_user, user_name)
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
                cmdb_role=[x for x in cmdb_role  if i['cmdb_id']  in x]

                i['role_id']=list(set([a[6] for a in cmdb_role ]))
                i['role_name']=list(set([b[5] for b in cmdb_role]))

                keys = QueryEntity(
                    UserRole.role_id,
                    Role.role_name,
                    User.login_user,
                    User.user_name
                )
                role_user = session.query(*keys). \
                    join(Role, UserRole.role_id == Role.role_id). \
                    join(User, UserRole.login_user == User.login_user)

                role_user=[list(x) for x in role_user]
                role_user=[y for y in role_user for d in i['role_id'] if d in y]

                i['nanotubes_login_user']=list(set([c[2] for c in role_user]))
                i['nanotubes_user_name']=list(set([d[3] for d in role_user]))

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
            Optional("sid"): scm_str
        }))
        params["create_owner"] = self.current_user
        with make_session() as session:
            new_cmdb = CMDB(**params)

            # 检测数据库是否有重复信息
            if session.query(CMDB).filter_by(connect_name=params["connect_name"]).first():
                self.resp_bad_req(msg="连接名称已存在")
                return

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
            Optional("sid"): scm_str
        }))
        cmdb_id = params.pop("cmdb_id")
        with make_session() as session:
            the_cmdb = session.query(CMDB).filter_by(cmdb_id=cmdb_id).first()
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
            session.query(TaskManage).filter_by(**params).delete()
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
                # bound_q = session.query(RoleDataPrivilege.schema_name).\
                #     filter(RoleDataPrivilege.role_id == role_id)
                # if cmdb_id:
                #     bound_q = bound_q.filter(RoleDataPrivilege.cmdb_id == cmdb_id)
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
                # 如果没有给出cmdb_id，则去查找最近的健康度，把最差的前十个拿出来
                worst_10 = cmdb_utils.get_latest_health_score_cmdb(
                    session=session, user_login=self.current_user)[:10]
                cmdb_connect_name_list = [i["connect_name"] for i in worst_10]
            else:
                cmdb_connect_name_list = [i[0] for i in session.query(CMDB.connect_name).
                    filter(CMDB.cmdb_id.in_(cmdb_id_list))]
            fields = set()
            ret = defaultdict(dict)  # {date: [{health data}, ...]}
            for cn in cmdb_connect_name_list:
                dh_q = session.query(DataHealth).filter(
                    DataHealth.database_name == cn,
                    DataHealth.collect_date > now.shift(weeks=-1).datetime
                ).order_by(DataHealth.collect_date)
                for dh in dh_q:
                    ret[dh.collect_date.date()][dh.database_name] = dh.health_score
                    fields.add(dh.database_name)
            base_lines = [i[0] for i in session.query(CMDB.baseline).
                filter(CMDB.connect_name.in_(cmdb_connect_name_list)).
                order_by(CMDB.baseline)]
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

    def patch(self):
        """局部修改评分的数据库，schema"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "schema_names": [scm_unempty_str]
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_names = params.pop("schema_names")
        del params

        with make_session() as session:
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).first()
            if not cmdb:
                return self.resp_bad_req(msg="cmdb不存在")
            try:
                schemas = get_cmdb_available_schemas(cmdb)
            except cx_Oracle.DatabaseError as err:
                print(err)
                return self.resp_bad_req(msg="无法连接到目标主机")
            schema_delta = set(schema_names) - set(schemas)
            if schema_delta:
                print(schemas)
                return self.resp_bad_req(msg=f"给出的schema中包含该库不存在的schema：{schema_delta}")
            session.query(DataHealthUserConfig). \
                filter(DataHealthUserConfig.database_name == cmdb.connect_name). \
                delete(synchronize_session='fetch')
            session.add_all([DataHealthUserConfig(
                database_name=cmdb.connect_name,
                username=i
            ) for i in schema_names])
        self.resp_created(msg="评分配置成功")

    def delete(self):
        """删除需要评分的库"""
        params = self.get_json_args(Schema({
            'database_name': scm_unempty_str,
            'username': scm_unempty_str
        }))
        with make_session() as session:
            session.query(DataHealthUserConfig).filter_by(**params).delete()
        self.resp_created("删除评分schema成功")
