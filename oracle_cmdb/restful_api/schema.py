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

