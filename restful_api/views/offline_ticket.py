# Author: kk.Fang(fkfkbill@gmail.com)

import uuid

import chardet
from schema import Schema, Optional, And, Or

from utils.schema_utils import *
from utils.datetime_utils import *
from utils.offline_utils import *
from utils.const import *
from utils.parsed_sql import ParsedSQL
from models.mongo import *
from models.oracle import *
from utils import cmdb_utils
from task.mail_report import timing_send_work_list_status
from task.offline_ticket import offline_ticket


class TicketOuterHandler(TicketReq):

    def get(self):
        """线下工单的外层归档列表，按照日期，工单类型，审核结果来归档"""
        self.resp()


class TicketHandler(TicketReq):

    def get(self):
        """工单列表"""
        self.resp()

    def post(self):
        """提交工单"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            Optional("schema_name", default=None): scm_unempty_str,
            "audit_role_id": scm_gt0_int,
            Optional("task_name", default=None): scm_unempty_str,
            "session_id": scm_unempty_str,
            Optional("online_username", default=None): scm_str,
            Optional("online_password", default=None): scm_str
        }))
        params["submit_owner"] = self.current_user
        session_id = params.pop("session_id")

        with make_session() as session:
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == params["cmdb_id"]).first()

            if cmdb.database_type == DB_ORACLE:

                sub_ticket_analysis = OracleSubTicketAnalysis()
                if not cmdb_utils.check_cmdb_privilege(cmdb):
                    return self.resp_forbidden(
                        msg=f"当前纳管库的登录用户({cmdb.user_name})权限不足，"
                            "无法做诊断分析。"
                    )
                if not params["schema_name"]:
                    # 缺省就用纳管库登录的用户去执行动态审核（也就是explain plan for）
                    # 缺省的情况下，假设用户会在自己上传的sql语句里带上表的schema
                    # 如果他的sql不带上schema，则它必须在提交工单的时候指定sql运行的schema_name
                    # 否则无法确定他的对象是处在哪个schema下面的
                    # 默认的纳管库用户是需要打开权限的，以保证能够在访问别的schema的对象
                    # 所以需要在前面先验证纳管库登录的用户是否有足够的权限。
                    params["schema_name"] = cmdb.user_name
                params["system_name"] = cmdb.business_name
                params["database_name"] = cmdb.connect_name
                if not params["task_name"]:
                    params['task_name'] = sub_ticket_analysis.get_available_task_name(
                        submit_owner=params["submit_owner"]
                    )
                ticket = WorkList(**params)
                session.add(ticket)
                session.commit()
                session.refresh(ticket)
                # TODO
                # offline_ticket.delay(work_list_id=ticket.work_list_id, sqls=sqls)

            elif cmdb.database_type == DB_MYSQL:

                pass

            self.resp_created(msg="已安排分析，请稍后查询分析结果。")

    def patch(self):
        """编辑工单状态"""
        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL)

        params = self.get_json_args(Schema({
            "work_list_id": scm_int,
            Optional("audit_comments"): scm_str,
            "work_list_status": And(scm_int,
                                    scm_one_of_choices(ALL_OFFLINE_TICKET_STATUS))
        }))
        params["audit_date"] = datetime.now()
        params["audit_owner"] = self.current_user
        work_list_id = params.pop("work_list_id")
        with make_session() as session:
            session.query(WorkList).\
                filter(WorkList.work_list_id == work_list_id).\
                update(params)
            ticket = session.query(WorkList).\
                filter(WorkList.work_list_id == work_list_id).\
                first()
            timing_send_work_list_status.delay(ticket.to_dict())
        return self.resp_created(msg="更新成功")

    def delete(self):
        """删除工单"""
        self.resp()


class TicketExportHandler(TicketReq):

    def get(self):
        """导出工单"""
        self.resp()


class SQLUploadHandler(TicketReq):

    def get(self):
        """获取上传的临时sql数据"""
        params = self.get_query_args(Schema({
            "session_id": scm_str,
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        with make_session() as session:
            q = session.query(WorkListAnalyseTemp).filter_by(**params)
            if keyword:
                q = self.query_keyword(q, keyword,
                                       WorkListAnalyseTemp.sql_text,
                                       WorkListAnalyseTemp.comments)
            sqls, p = self.paginate(q, **p)
            self.resp([sql.to_dict() for sql in sqls], **p)

    def post(self):
        """上传一个sql脚本"""
        # TODO 目前没有对oracle和mysql的上传作区分，未来可能要考虑这点

        if not len(self.request.files) or not self.request.files.get("file"):
            return self.resp_bad_req(msg="未选择文件。")
        params = self.get_query_args(Schema({
            Optional("filter_sql_type", default=None):
                And(scm_int, scm_one_of_choices(ALL_SQL_TYPE)),
        }))
        file_object = self.request.files.get("file")[0]
        filter_sql_type = params.pop("filter_sql_type")

        # 现在仅支持SQL脚本文件，不再支持Excel文档
        body = file_object["body"]
        try:
            body = body.decode(chardet.detect(body))
        except UnicodeDecodeError:
            body = body.decode('utf-8')
        parsed_sql_obj = ParsedSQL(body)
        with make_session() as session:
            session_id = uuid.uuid4().hex
            to_add = [
                WorkListAnalyseTemp(
                    session_id=session_id,
                    sql_text=obj.normalized,
                    comments="",
                    sql_type=obj.sql_type,
                    num=i
                ) for i, obj in enumerate(parsed_sql_obj)
            ]
            TicketMeta(
                session_id=session_id,
                original_sql=parsed_sql_obj.get_original_sql(),
                comment_striped_sql=parsed_sql_obj.get_comment_striped_sql()
            ).save()
            session.add_all(to_add)
            self.resp_created({"session_id": session_id})
        self.resp()

    def patch(self):
        """编辑上传的临时sql数据"""
        params = self.get_json_args(Schema({
            "id": scm_str,
            Optional("sql_text"): scm_unempty_str,
            Optional("comments"): scm_str,
            Optional("sql_type"): scm_one_of_choices(ALL_SQL_TYPE),
            Optional("delete", default=False): scm_bool
        }))
        wlat_id = params.pop("id")
        delete = params.pop("delete")
        with make_session() as session:
            wlat = session.query(WorkListAnalyseTemp).filter_by(id=wlat_id).first()
            if not wlat:
                self.resp_bad_req(msg=f"找不到编号为{wlat_id}的临时sql session")
            if delete:
                session.delete(wlat)
                session.commit()
                self.resp_created(msg="sql已删除。")
            else:
                wlat.from_dict(params)
                session.add(wlat)
                session.commit()
                session.refresh(wlat)
                self.resp_created(wlat.to_dict())
