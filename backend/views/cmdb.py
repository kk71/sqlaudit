# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And, Or
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_

import settings
from backend.utils.schema_utils import *
from backend.views.base import *
from backend.models.oracle import *
from backend.utils import cmdb_utils


class CMDBHandler(AuthReq):

    def get(self):
        params = self.get_query_args(Schema({
            Optional("page", default=1): scm_int,
            Optional("per_page", default=10): scm_int,
            Optional("keyword", default=None): scm_unempty_str,
            Optional("current", default=False): scm_bool  # 只返回当前登录用户可见的cmdb
        }))
        keyword = params.pop("keyword")
        current = params.pop("current")
        with make_session() as session:
            q = session.query(CMDB)
            if keyword:
                q = self.query_keyword(q, keyword,
                                   CMDB.cmdb_id,
                                   CMDB.connect_name,
                                   CMDB.group_name,
                                   CMDB.business_name,
                                   CMDB.machine_room,
                                   CMDB.server_name
                                   )
            if current:
                current_cmdb_ids = cmdb_utils.get_current_cmdb(session, self.current_user)
                q = q.filter(CMDB.cmdb_id.in_(current_cmdb_ids))
            items, p = self.paginate(q, **params)
            self.resp([i.to_dict() for i in items], **p)

    def check_for_cmdb_integrity(self, session):
        """检查cmdb的数据的统一形制"""
        return True or False

    def post(self):
        """增加CMDB"""
        params = self.get_json_args(Schema({
            "connect_name": scm_unempty_str,
            "group_name": scm_str,
            "business_name": scm_str,
            "machine_room": scm_int,
            "database_type": scm_int,
            "server_name": scm_str,
            "ip_address": scm_unempty_str,
            "port": scm_int,
            "service_name": scm_str,
            "user_name": scm_unempty_str,
            "password": scm_unempty_str,
            "is_collect": scm_bool,
            "status": scm_bool,
            "auto_sql_optimized": scm_bool,
            "domain_env": scm_int,
            "is_rac": scm_bool,
            "white_list_status": scm_bool,
            "while_list_rule_counts": scm_int,
            "db_model": scm_unempty_str,
            "baseline": scm_int
        }))
        params["create_owner"] = self.current_user
        with make_session() as session:
            new_cmdb = CMDB(**params)

            # 检测数据库是否有重复信息
            if session.query(CMDB).filter_by(connect_name=params["connect_name"]).first():
                self.resp_bad_req(msg="连接名称已存在")

            # TODO 需要连接数据库做测试

            session.add(new_cmdb)
            session.commit()
            session.refresh(new_cmdb)
            self.resp_created(new_cmdb.to_dict())

    def patch(self):
        """修改CMDB"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_unempty_str,

            Optional("connect_name"): scm_unempty_str,
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
        self.resp_created(msg="已删除。")


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
                schema_names = session.query(DataPrivilege).\
                    filter_by(cmdb_id=cmdb_id).\
                    with_entities(DataPrivilege.schema_name)
                self.resp(list({i.schema_name for i in schema_names}))
