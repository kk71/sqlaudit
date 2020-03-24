# Author: kk.Fang(fkfkbill@gmail.com)

import abc

from utils.schema_utils import *

from .base import *


class UploadTempScriptHandler(TicketReq, abc.ABCMeta):

    def get(self):
        """获取上传的临时sql数据"""
        params = self.get_query_args(Schema({
            "script_id": scm_str,
            scm_optional("keyword", default=None): scm_str,
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

    def patch(self):
        """编辑上传的临时sql数据"""
        params = self.get_json_args(Schema({
            "id": scm_str,
            scm_optional("sql_text"): scm_unempty_str,
            scm_optional("comments"): scm_str,
            scm_optional("sql_type"): scm_one_of_choices(ALL_SQL_TYPE),
            scm_optional("delete", default=False): scm_bool
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

    @abc.abstractmethod
    def post(self):
        """上传脚本"""
        pass
