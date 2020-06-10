__all__ = [
    "CMDBHandler",
    "CMDBAggregationHandler"
]

from typing import List, Dict
from collections import defaultdict

from sqlalchemy import or_

import utils.const
import cmdb.const
from ..const import *
from cmdb.cmdb import *
from cmdb.cmdb_task import CMDBTask
from auth.user import *
from auth.product_license import *
from utils.schema_utils import *
from models.sqlalchemy import *
from ticket.ticket import *
from ticket.sub_ticket import SubTicket
from restful_api.modules import as_view
from oracle_cmdb.cmdb import *
from rule.cmdb_rule import CMDBRule
from rule.rule_cartridge import RuleCartridge
from oracle_cmdb.auth.user_utils import current_cmdb
from oracle_cmdb.auth.role import RoleOracleCMDBSchema
from oracle_cmdb.restful_api.base import *
from oracle_cmdb.statistics import *
from oracle_cmdb.tasks.capture import OracleCMDBTaskCapture


@as_view(group="cmdb")
class CMDBHandler(OraclePrivilegeReq):

    def get(self):
        """cmdb列表"""

        params = self.get_query_args(Schema({
            # 精确匹配
            scm_optional("cmdb_id"): scm_gt0_int,
            scm_optional("connect_name"): scm_unempty_str,
            scm_optional("group_name"): scm_unempty_str,
            scm_optional("business_name"): scm_unempty_str,

            # 模糊匹配多个字段
            scm_optional("keyword", default=None): scm_str,

            # 排序
            scm_optional("sort", default=utils.const.SORT_DESC): And(
                scm_str,
                self.scm_one_of_choices(utils.const.ALL_SORTS)
            ),

            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        sort = params.pop("sort")
        p = self.pop_p(params)

        with make_session() as session:
            cmdb_q = self.cmdbs(session).filter_by(**params)
            if keyword:
                cmdb_q = self.query_keyword(cmdb_q, keyword,
                                            CMDB.cmdb_id,
                                            CMDB.connect_name,
                                            CMDB.group_name,
                                            CMDB.business_name,
                                            CMDB.server_name)
            # 构建输出的纳管库信息，带上评分
            all_current_cmdb: Dict[int, dict] = {}
            last_cmdb_score = OracleCMDBTaskCapture.last_cmdb_score(session)
            for a_cmdb in cmdb_q:
                all_current_cmdb[a_cmdb.cmdb_id] = {
                    "data_health": last_cmdb_score[a_cmdb.cmdb_id],
                    "rating_schemas": a_cmdb.rating_schemas(),
                    "rating_schemas_num": len(a_cmdb.rating_schemas()),
                    **a_cmdb.to_dict()
                }
            all_current_cmdb_list: List = list(all_current_cmdb.values())
            all_current_cmdb_list = sorted(
                all_current_cmdb_list,
                key=lambda k: k["data_health"].get("score", 0)
            )
            if sort == utils.const.SORT_DESC:
                all_current_cmdb_list.reverse()
            ret, p = self.paginate(all_current_cmdb_list, **p)

            # 对分页之后的纳管库列表补充额外数据
            last_cmdb_task_record_id_dict = OracleCMDBTaskCapture. \
                last_login_user_entry_cmdb(
                session,
                self.current_user
            )
            for i in ret:
                i["stats"] = last_cmdb_task_record_id_dict[i["cmdb_id"]]

            # 给ret加上纳管它的角色信息（角色名，角色id）
            #  以及纳管它的用户(login_user, user_name)
            for i in ret:
                # 绑定当前库的角色
                cmdb_role = session.query(*(cmdb_role_qe := QueryEntity(
                    Role.role_name,
                    Role.role_id
                ))).filter(
                    Role.role_id == RoleOracleCMDBSchema.role_id,
                    RoleOracleCMDBSchema.cmdb_id == i["cmdb_id"]
                )
                i["role"] = [cmdb_role_qe.to_dict(i) for i in cmdb_role]
                role_ids = [i["role_id"] for i in i["role"]]

                # 绑定当前库的用户
                role_user = session.query(*(cmdb_user_qe := QueryEntity(
                    User.login_user,
                    User.username
                ))).filter(
                    UserRole.role_id.in_(role_ids),
                    UserRole.login_user == User.login_user)
                i['user'] = [cmdb_user_qe.to_dict(i) for i in role_user]
            self.resp(ret, **p)

    get.argument = {
        "querystring": {
            "//cmdb_id": 1,
            "//connect_name": "",
            "//group_name": "",
            "//business_name": "",
            "keyword": "",
            "sort": "desc",
            "page": 1,
            "per_page": 10
        }
    }

    def post(self):
        """增加CMDB"""
        params = self.get_json_args(Schema({
            "connect_name": scm_unempty_str,
            "group_name": scm_str,
            "business_name": scm_str,
            "db_type": self.scm_one_of_choices(cmdb.const.ALL_DB_TYPE),
            "server_name": scm_str,
            "ip_address": scm_unempty_str,
            "port": scm_int,
            "service_name": scm_str,
            "username": scm_unempty_str,
            "password": scm_unempty_str,
            scm_optional("status", default=True): scm_bool,
            scm_optional("domain_env"): scm_int,
            scm_optional("is_rac"): scm_bool,
            "db_model": self.scm_one_of_choices(cmdb.const.ALL_DB_MODEL),
            "baseline": scm_int,
            "is_pdb": scm_bool,
            "version": scm_unempty_str,
            scm_optional("sid"): scm_str,
            scm_optional("allow_online", default=False): scm_bool
        }))
        with make_session() as session:

            cmdb_count = len(session.query(CMDB).all())
            license_key = SqlAuditLicenseKeyManager.latest_license_key(session)
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

            # 增加库的规则
            rules = RuleCartridge.filter(
                db_type=new_cmdb.db_type, db_model=new_cmdb.db_model)
            cmdb_rules = []
            for rule in rules:
                cmdb_rule = CMDBRule(cmdb_id=new_cmdb.cmdb_id)
                cmdb_rule.from_rule_cartridge(rule, force=True)
                cmdb_rules.append(cmdb_rule)
            CMDBRule.objects.insert(cmdb_rules)
            self.resp_created(new_cmdb.to_dict())

    post.argument = {
        "json": {
            "connect_name": "",
            "group_name": "",
            "business_name": "",
            "db_type": "oracle",
            "server_name": "",
            "ip_address": "",
            "port": "",
            "service_name": "",
            "username": "",
            "password": "",
            "status": 1,
            "domain_env": "",
            "is_rac": 1,
            "db_model": "OLTP",
            "baseline": "0.8",
            "is_pdb": 1,
            "version": "",
            "sid": "",
            "allow_online": 1,
        }
    }

    def patch(self):
        """修改CMDB"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,

            scm_optional("ip_address"): scm_unempty_str,
            scm_optional("port"): scm_int,
            scm_optional("service_name"): scm_str,
            scm_optional("group_name"): scm_str,
            scm_optional("business_name"): scm_str,
            scm_optional("db_type"): self.scm_one_of_choices(cmdb.const.ALL_DB_TYPE),
            scm_optional("server_name"): scm_str,
            scm_optional("username"): scm_unempty_str,
            scm_optional("password"): scm_unempty_str,
            scm_optional("status"): scm_bool,
            scm_optional("domain_env"): scm_int,
            scm_optional("is_rac"): scm_bool,
            scm_optional("db_model"): self.scm_one_of_choices(cmdb.const.ALL_DB_MODEL),
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
                        OracleCMDB.service_name == params["service_name"],
                        OracleCMDB.sid == params["sid"]
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

            session.add(the_cmdb)
            session.commit()
            session.refresh(the_cmdb)
            self.resp_created(the_cmdb.to_dict())

    patch.argument = {
        "json": {
            "cmdb_id": 1,
            "ip_address": "",
            "port": 1,
            "service_name": "",
            "group_name": "",
            "business_name": "",
            "db_type": "oracle",
            "server_name": "",
            "username": "",
            "password": "",
            "status": 1,
            "domain_env": 1,
            "is_rac": 1,
            "db_model": "OLTP",
            "baseline": 80,
            "is_pdb": 1,
            "version": "",
            "sid": "",
            "allow_online": 1
        }
    }

    def delete(self):
        """删除CMDB"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
        }))
        with make_session() as session:
            the_cmdb = session.query(CMDB).filter_by(**params).first()
            session.delete(the_cmdb)
            session.query(CMDBTask).filter_by(**params).delete(synchronize_session=False)
            session.query(RoleOracleCMDBSchema).filter_by(**params).delete(synchronize_session=False)
            Ticket.filter(**params).delete()
            SubTicket.filter(**params).delete()
        self.resp_created(msg="已删除。")

    def options(self):
        """测试连接是否成功"""
        params = self.get_query_args(Schema({
            "cmdb_id": scm_int
        }))
        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(**params).first()
            resp = cmdb.test_connectivity()
            self.resp(resp)

    options.argument = delete.argument = {
        "querystring": {
            "cmdb_id": 2526
        }
    }


@as_view("aggregation", group="cmdb")
class CMDBAggregationHandler(OraclePrivilegeReq):

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
                    CMDB.cmdb_id.in_(current_cmdb(session, self.current_user)))
            for i, k in enumerate(key):
                for qr in query_ret:
                    ret[k].add(qr[i])
            ret = {k: list(v) for k, v in ret.items()}
            self.resp(ret)

    get.argument = {
        "querystring": {
            "key": "connect_name,group_name,business_name"
        }
    }
