# Author: kk.Fang(fkfkbill@gmail.com)

import cx_Oracle

from ..cmdb import *
from ..rate import OracleRatingSchema
from ..auth.user_utils import *
from utils.schema_utils import *
from utils.conc_utils import *
from restful_api.modules import *
from models.sqlalchemy import *
from auth.restful_api.base import *


@as_view("rating", group="schema")
class RatingSchemaHandler(AuthReq):

    def get(self):
        """获取需要评分的schema列表"""
        params = self.get_query_args(Schema({
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")
        del params

        with make_session() as session:
            rating = session.query(*(qe := QueryEntity(
                    OracleCMDB.connect_name,
                    OracleRatingSchema.cmdb_id,
                    OracleRatingSchema.schema_name,
                    OracleRatingSchema.weight
                ))).join(
                OracleCMDB, OracleCMDB.cmdb_id == OracleRatingSchema.cmdb_id)
            if keyword:
                rating = self.query_keyword(rating, keyword,
                                            OracleCMDB.connect_name,
                                            OracleRatingSchema.schema_name,
                                            OracleRatingSchema.weight)
            items, p = self.paginate(rating, **p)
            items = sorted([qe.to_dict(i) for i in items], key=lambda k: k["cmdb_id"])
            self.resp(items, **p)

    get.argument = {
        "querystring": {
            "//keyword": "emm",
            "//page": 1,
            "//per_page": 10
        }
    }

    def post(self):
        """以库为单位修改(增删)数据评分的schema,默认权重为1"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "schema_names": [scm_unempty_str]
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_names = set(params.pop("schema_names"))
        del params

        with make_session() as session:
            cmdb = session.query(OracleCMDB).filter(OracleCMDB.cmdb_id == cmdb_id).first()
            import cx_Oracle
            try:
                schemas = cmdb.get_available_schemas()
            except cx_Oracle.DatabaseError as err:
                print(err)
                return self.resp_bad_req(msg="无法连接到目标主机")
            schema_delta_not_existed = schema_names - set(schemas)
            if schema_delta_not_existed:
                print(schemas)
                return self.resp_bad_req(
                    msg="给出的schema中包含该库不存在的schema："
                        f"{', '.join([i for i in schema_delta_not_existed])}")

            qe = QueryEntity(OracleRatingSchema.schema_name)
            schema_names_current = qe.to_plain_list(
                session.query(*qe).filter(
                    OracleRatingSchema.cmdb_id == cmdb.cmdb_id))
            schema_names_current = set(schema_names_current)
            schema_names_to_delete = schema_names_current.difference(schema_names)
            schema_names_to_add = schema_names.difference(schema_names_current)
            session.query(OracleRatingSchema).filter(
                OracleRatingSchema.cmdb_id == cmdb.cmdb_id,
                OracleRatingSchema.schema_name.in_(list(schema_names_to_delete))
            ).delete(synchronize_session='fetch')
            session.add_all([OracleRatingSchema(
                cmdb_id=cmdb.cmdb_id,
                schema_name=i,
                weight=1.0
            ) for i in schema_names_to_add])
        self.resp_created(msg="评分配置成功")

    post.argument = {
        "json": {
            "cmdb_id": 2526,
            "schema_names": ["APES"]
        }
    }

    def patch(self):
        """以cmdb-schema为单位修改评分权重"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            "weight": And(scm_float, lambda x: x <= 1)
        }))
        cmdb_id = params.pop("cmdb_id")
        schema_name = params.pop("schema_name")

        with make_session() as session:
            cmdb = session.query(OracleCMDB).filter(OracleCMDB.cmdb_id == cmdb_id).first()
            session.query(OracleRatingSchema).filter_by(
                cmdb_id=cmdb.cmdb_id,
                schema_name=schema_name,
            ).update(params)
        self.resp_created(msg="评分配置更新成功")

    patch.argument = {
        "json": {
            "cmdb_id": 2526,
            "schema_name": "APEX",
            "weight": 1
        }
    }

    def delete(self):
        """删除不需要评分的库的schema"""
        params = self.get_json_args(Schema({
            'cmdb_id': scm_unempty_str,
            'schema_name': scm_unempty_str
        }))
        with make_session() as session:
            session.query(OracleRatingSchema). \
                filter_by(**params).delete(synchronize_session=False)
        self.resp_created("删除评分schema成功")

    delete.argument = {
        "json": {
            "cmdb_id": 2526,
            "schema_name": "APEX"
        }
    }


@as_view("schema", group="schema")
class SchemaHandler(AuthReq):

    async def get(self):
        """获取schema列表"""

        DATA_SCHEMA_PRIVILEGE = "data_schema_privilege"
        RATING_SCHEMA_PRIVILEGE = "rating_schema_privilege"

        params = self.get_query_args(Schema({
            scm_optional("cmdb_id", default=None): scm_int,
            scm_optional("connect_name", default=None): scm_unempty_str,
            scm_optional("current", default=not self.is_admin()): scm_bool,
            scm_optional("divide_by", default=None): scm_one_of_choices((
                DATA_SCHEMA_PRIVILEGE,  # 以login_user区分当前库的数据权限（绑定、未绑定）
                RATING_SCHEMA_PRIVILEGE  # 以login_user区分当前库的评分权限（绑定、未绑定）
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
                cmdb = session.query(OracleCMDB).filter_by(connect_name=connect_name).first()
                cmdb_id = cmdb.cmdb_id
            if login_user and divide_by == DATA_SCHEMA_PRIVILEGE:
                # 返回给出的用户所绑定bound的schema，以及未绑定else的
                bound = await async_thr(
                    current_schema, login_user, cmdb_id)
                cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()

                try:
                    all_schemas = await async_thr(
                        cmdb.get_available_schemas)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                await self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif role_id and divide_by == DATA_SCHEMA_PRIVILEGE:
                # 返回给出的角色所绑定的schema，以及未绑定的
                bound_schema_info = await async_thr(
                    current_schema, cmdb_id=cmdb_id, verbose=True)
                bound = list({schema_name for _, _, _, schema_name in bound_schema_info})
                cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        cmdb.get_available_schemas)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                await self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif divide_by == RATING_SCHEMA_PRIVILEGE:
                # 返回给出的库需要加入数据评分的schema，以及不需要的
                if connect_name:  # TODO
                    bound = session.query(OracleRatingSchema.schema_name). \
                        filter(OracleRatingSchema.cmdb_id == cmdb_id)
                elif cmdb_id:
                    bound = session.query(OracleRatingSchema.schema_name). \
                        join(OracleCMDB, OracleRatingSchema.cmdb_id == OracleCMDB.cmdb_id). \
                        filter(OracleCMDB.cmdb_id == cmdb_id)
                else:
                    assert 0
                cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
                bound = [i[0] for i in bound]
                try:
                    all_schemas = await async_thr(
                        cmdb.get_available_schema)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                await self.resp({
                    "bound": bound,
                    "else": [i for i in all_schemas if i not in bound]
                })

            elif current:
                # 当前登录用户可用(数据权限配置)的schema
                current_schemas = await async_thr(
                    current_schema, self.current_user, cmdb_id)
                await self.resp(current_schemas)

            else:
                # 当前cmdb的全部的schema，不考虑数据权限
                cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
                try:
                    all_schemas = await async_thr(
                        cmdb.get_available_schemas)
                except cx_Oracle.DatabaseError as err:
                    return self.resp_bad_req(msg="无法连接到数据库")
                await self.resp(all_schemas)

    get.argument = {
        "querystring": {
            "//cmdb_id": 2526,
            "//connect_name": "emmm",
            "//divide_by": "data_schema_privilege",
            "//login_user": "",
            "//role_id": 1,
            "//current": 0
        }
    }
