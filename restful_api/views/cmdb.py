# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle
from collections import defaultdict

from schema import Schema, Optional, And

from utils.schema_utils import *
from utils.perf_utils import *
from utils.datetime_utils import *
from utils.cmdb_utils import get_cmdb_available_schemas
from .base import *
from utils import cmdb_utils
from models.oracle import *

from plain_db.oracleob import OracleOB


class CMDBHandler(AuthReq):

    def get(self):
        """查询纳管数据库列表"""
        params = self.get_query_args(Schema({
            Optional("current", default=False): scm_bool,  # 只返回当前登录用户可见的cmdb

            # 精确匹配
            Optional("cmdb_id"): scm_int,
            Optional("connect_name"): scm_unempty_str,
            Optional("group_name"): scm_unempty_str,
            Optional("business_name"): scm_unempty_str,

            # 模糊匹配多个字段
            Optional("keyword", default=None): scm_unempty_str,

            # 分页
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        keyword = params.pop("keyword")
        current = params.pop("current")
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
                current_cmdb_ids = cmdb_utils.get_current_cmdb(session, self.current_user)
                q = q.filter(CMDB.cmdb_id.in_(current_cmdb_ids))
                all_db_data_health = cmdb_utils.get_latest_health_score_cmdb(session, self.current_user)
            else:
                all_db_data_health = cmdb_utils.get_latest_health_score_cmdb(session)
            ret = []
            if "cmdb_id" in params.keys():
                p = {}
                cmdb_dict = q.first().to_dict()
                for data_health in all_db_data_health:
                    if data_health["connect_name"] == cmdb_dict["connect_name"]:
                        ret.append({
                            **cmdb_dict,
                            "data_health": data_health
                        })
                        break
            else:
                db_data_health, p = self.paginate(all_db_data_health, **p)
                for data_health in db_data_health:
                    cmdb_obj_of_this_dh = q.filter(CMDB.connect_name == data_health["connect_name"]). \
                        first()
                    if not cmdb_obj_of_this_dh:
                        continue
                    ret.append({
                        **cmdb_obj_of_this_dh.to_dict(),
                        "data_health": data_health
                    })
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
            "baseline": scm_int
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
            for task_type in cmdb_utils.ALL_DB_TASKS:
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
                    task_status=True,
                    task_exec_scripts=task_type,
                    **task_dict
                )
                session.add(new_task)

            session.commit()
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
            Optional("baseline"): scm_int
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
        self.resp_created(msg="已删除。")

    def options(self):
        """测试连接是否成功"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int
        }))
        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(**params).first()
            self.resp(cmdb_utils.test_cmdb_connectivity(cmdb))


class CMDBAggregationHandler(AuthReq):

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
            for i, k in enumerate(key):
                for qr in query_ret:
                    ret[k].add(qr[i])
            ret = {k: list(v) for k, v in ret.items()}
            self.resp(ret)


class SchemaHandler(AuthReq):

    def get(self):
        """获取schema列表"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int,
            Optional("current", default=False): scm_bool
        }))
        cmdb_id = params.pop("cmdb_id")
        current = params.pop("current")
        with make_session() as session:
            if current:
                current_schemas = cmdb_utils.get_current_schema(session,
                                                                self.current_user,
                                                                cmdb_id)
                self.resp(current_schemas)
            else:
                schema_names = session.query(DataPrivilege). \
                    filter_by(cmdb_id=cmdb_id). \
                    with_entities(DataPrivilege.schema_name)
                self.resp(list({i.schema_name for i in schema_names}))


class CMDBHealthTrendHandler(AuthReq):

    @timing
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
                cmdb_connect_name_list = [i["connect_name"]
                                          for i in cmdb_utils.get_latest_health_score_cmdb(session)[:10]]
            else:
                cmdb_connect_name_list = [i[0] for i in session.query(CMDB.connect_name).
                                                    filter(CMDB.cmdb_id.in_(cmdb_id_list))]
            ret = defaultdict(dict)  # {date: [{health data}, ...]}
            for cn in cmdb_connect_name_list:
                dh_q = session.query(DataHealth).filter(
                    DataHealth.database_name == cn,
                    DataHealth.collect_date > now.shift(weeks=-1).datetime,
                    DataHealth.collect_date <= now.datetime
                ).order_by(DataHealth.collect_date)
                for dh in dh_q:
                    ret[dh.collect_date.date()][dh.database_name] = dh.health_score
            if len(ret) > 0:
                base_lines = [i[0] for i in session.query(CMDB.baseline).
                                filter(CMDB.connect_name.in_(cmdb_connect_name_list)).
                                order_by(CMDB.baseline)]
                if not base_lines or base_lines[0] == 0:
                    base_line = 80
                else:
                    base_line = base_lines[0]
            else:
                assert 0
            ret = [{
                "date": d_to_str(k),
                "基线": base_line,
                **v
            } for k, v in ret.items()]
            self.resp(ret)


class RankingConfigHandler(AuthReq):

    def get(self):
        """获取需要评分的数据库列表"""
        params = self.get_query_args(Schema({
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
        }))
        p = self.pop_p(params)
        del params

        if self.current_user != "admin":
            return self.resp_bad_req(msg="No Authority")

        with make_session() as session:
            ranking = session.query(
                CMDB.connect_name,
                DataHealthUserConfig.database_name,
                DataHealthUserConfig.username,
                DataHealthUserConfig.needcalc,
                DataHealthUserConfig.weight). \
                join(CMDB, DataHealthUserConfig.database_name == CMDB.connect_name)
            rankings = []
            for x in ranking:
                rankings.append({
                    'connect_name': x[0],
                    'databese_name': x[1],
                    'username': x[2],
                    'needcalc': x[3],
                    'weight': x[4]
                })

            rankings_this_page, p = self.paginate(rankings, **p)

            # ?这个数据应该可不返回
            cmdbs = session.query(CMDB).all()
            cmdbs = [cmdb.to_dict() for cmdb in cmdbs]

            self.resp(content={'rankings': rankings_this_page, 'cmdb': cmdbs}, **p)

    def patch(self):
        """局部修改评分的数据库，schema"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "database_name": scm_unempty_str,
            "schema_names": list  # 结合前端情况可调整
        }))
        cmdb_id = params.pop("cmdb_id")
        database_name = params.pop("database_name")
        schema_names = params.pop("schema_names")
        del params
        with make_session() as session:
            cmdb_objects = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).all()
            for cmdb_object in cmdb_objects:
                try:
                    schemas = get_cmdb_available_schemas(cmdb_object)
                except cx_Oracle.DatabaseError as err:
                    print(err)
                    return self.resp_bad_req(msg="无法连接到目标主机")
            if set(schema_names) - set(schemas):
                return self.resp_bad_req(msg="参数不正确")

            existing_schemas = session.query(DataHealthUserConfig.username).\
                filter(DataHealthUserConfig.database_name == database_name)
            existing_schemas = [list(existing_schema)[0] for existing_schema in existing_schemas]
            loading, existing = set(schema_names), set(existing_schemas)

            insert_schemas = [x for x in (loading - existing)]
            delete_schemas = [x for x in (existing - loading)]
            if insert_schemas:
                for schema in insert_schemas:
                    config = DataHealthUserConfig()
                    config.database_name = database_name
                    config.username = schema
                    config.needcalc = 'Y'
                    config.weight = 1
                    session.add(config)
            if delete_schemas:
                session.query(DataHealthUserConfig).filter(DataHealthUserConfig.database_name == database_name,
                                                           DataHealthUserConfig.username.in_(delete_schemas)).delete(
                    synchronize_session='fetch')
            self.resp_created(msg="评分配置成功")

    def post(self):
        """修改评分的查询,schema"""
        params = self.get_json_args(Schema({
            'cmdb_id': scm_int,
            'database_name': scm_unempty_str,
        }))
        cmdb_id, database_name = params.pop('cmdb_id'), params.pop('database_name')
        del params

        with make_session() as session:
            cmdb_objects = session.query(CMDB).filter(CMDB.cmdb_id == cmdb_id).all()
            for cmdb_object in cmdb_objects:
                try:
                    schemas = get_cmdb_available_schemas(cmdb_object)
                except cx_Oracle.DatabaseError as err:
                    # print(err)
                    return self.resp_bad_req(msg="无法连接到目标主机")
            authed_schemas = session.query(DataHealthUserConfig.username).\
                filter(DataHealthUserConfig.database_name == database_name)
            authed_schemas = [list(authed_schema)[0] for authed_schema in authed_schemas]
            schemas = {schema: 1 if schema in authed_schemas else 0 for schema in schemas}

            self.resp_created({'schemas': schemas})

    def delete(self):
        """删除需要评分的库"""
        params = self.get_query_args(Schema({
            'database_name': scm_unempty_str,
            'schema_name': scm_unempty_str
        }))

        with make_session() as session:
            session.query(DataHealthUserConfig).filter_by(**params).delete()
            self.resp_created("删除评分schema成功")
