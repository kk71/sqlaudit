# Author: kk.Fang(fkfkbill@gmail.com)

import uuid

import chardet
from schema import Schema, Optional, And, Or

from utils.schema_utils import *
from utils.offline import *
from utils.const import *
from utils.parsed_sql import ParsedSQL
from models.mongo import *
from models.oracle import *


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
        self.resp()

    def patch(self):
        """编辑工单状态"""
        self.resp()

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
