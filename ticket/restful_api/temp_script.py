# Author: kk.Fang(fkfkbill@gmail.com)

import abc

import chardet

from utils.schema_utils import *
from .base import *
from ..const import *
from ..ticket import *


class UploadTempScriptHandler(TicketReq, abc.ABCMeta):

    def get(self):
        """获取上传的临时sql数据"""
        params = self.get_query_args(Schema({
            "script_id": scm_str,
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        script_id = params.pop("script_id")
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        del params

        q=TempScriptStatement.objects().\
            filter_by(script__script_id=script_id)
        if keyword:
            q = self.query_keyword(q, keyword,"normalized","comment")
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

        wlat=TempScriptStatement.objects().filter_by(id=wlat_id).first()
        if not wlat:
            self.resp_bad_req(msg=f"找不到编号为{wlat_id}的临时sql session")
        if delete:
            wlat.delete()
            self.resp_created(msg="sql已删除。")
        else:
            wlat.from_dict(params)
            wlat.save()
            self.resp_created(wlat.to_dict())

    def post(self):
        """上传一个sql脚本"""

        if not len(self.request.files) or not self.request.files.get("file"):
            return self.resp_bad_req(msg="未选择文件。")

        params = self.get_query_args(Schema({
            scm_optional("filter_sql_type", default=None):
                And(scm_int, scm_one_of_choices(utils.const.ALL_SQL_TYPE)),
        }))
        file_object = self.request.files.get("file")[0]
        filter_sql_type = params.pop("filter_sql_type")

        # 现在仅支持SQL脚本文件，不再支持Excel文档
        body = file_object["body"]
        if not body:
            return self.resp_bad_req(msg="空脚本。")
        try:
            body = body.decode(chardet.detect(body)["encoding"])
        except:
            try:
                body = body.decode('utf-8')
            except:
                try:
                    body = body.decode('gbk')
                except Exception as e:
                    return self.resp_bad_req(msg=f"文本解码失败: {e}")
        script_object = TicketScript(script_name=file_object["filename"])

        self.resp_created({"script_id": script_object})
