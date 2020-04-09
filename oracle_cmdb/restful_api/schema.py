# Author: kk.Fang(fkfkbill@gmail.com)

from restful_api.modules import *
from auth.restful_api.base import *
from utils.schema_utils import *
from models.sqlalchemy import *
from cmdb.restful_api.cmdb import *


@as_view("rating", group="cmdb")
class RatingSchemaHandler(AuthReq):

    def get(self):
        """获取需要评分的数据库列表"""
        params = self.get_query_args(Schema({
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        keyword = params.pop("keyword")
        del params

        with make_session() as session:
            qe = QueryEntity(
                CMDB.cmdb_id,
                OracleRatingSchema.schema,
                OracleRatingSchema.weight
            )
            # TODO
            rankings = session. \
                query(*qe). \
                join(CMDB, OracleRatingSchema.database_name == CMDB.connect_name)
            if keyword:
                rankings = self.query_keyword(rankings, keyword,
                                              CMDB.connect_name,
                                              OracleRatingSchema.username)
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
            qe = QueryEntity(OracleRatingSchema.schema)
            schema_names_current = qe.to_plain_list(
                session.query(*qe).filter(
                    OracleRatingSchema.database_name == cmdb.connect_name))
            schema_names_current = set(schema_names_current)
            schema_names_to_delete = schema_names_current.difference(schema_names)
            schema_names_to_add = schema_names.difference(schema_names_current)
            session.query(OracleRatingSchema).filter(
                OracleRatingSchema.database_name == cmdb.connect_name,
                OracleRatingSchema.schema.in_(list(schema_names_to_delete))
            ).delete(synchronize_session='fetch')
            session.add_all([OracleRatingSchema(
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
            session.query(OracleRatingSchema).filter_by(
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
            session.query(OracleRatingSchema). \
                filter_by(**params).delete(synchronize_session=False)
        self.resp_created("删除评分schema成功")


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

