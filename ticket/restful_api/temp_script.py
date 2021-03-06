# Author: kk.Fang(fkfkbill@gmail.com)

import abc

import chardet

import parsed_sql.const
from utils.schema_utils import *
from restful_api.modules import *
from .base import *
from ..ticket import *


@as_view(group="ticket")
class UploadTempScriptHandler(TicketReq, abc.ABC):

    def get(self):
        """获取上传的临时sql脚本数据"""

        params = self.get_query_args(Schema({
            "script_id": scm_str,
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        script_id = params.pop("script_id")
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        del params

        q = TempScriptStatement.filter(script__script_id=script_id)
        if keyword:
            q = self.query_keyword(q, keyword, "normalized", "comment")
        sqls, p = self.paginate(q, **p)
        self.resp([sql.to_dict() for sql in sqls], **p)

    get.argument = {
        "querystring": {
            "script_id": "7ac16b313ae8471e9ba3f34d35e1968e",
            "//keyword": "",
            "//page": 1,
            "//per_page": 10
        }
    }

    def patch(self):
        """编辑及删除上传的临时sql脚本一条sql数据"""

        params = self.get_json_args(Schema({
            "statement_id": scm_str,

            "sql_text": scm_unempty_str,
            scm_optional("comment"): scm_str,
            scm_optional("delete", default=False): scm_bool
        }))
        statement_id = params.pop("statement_id")
        delete = params.pop("delete")

        temp_scipt_statement_object = TempScriptStatement.filter(
            statement_id=statement_id).first()
        if not temp_scipt_statement_object:
            self.resp_bad_req(msg=f"找不到编号为{statement_id}的临时sql语句")
        if delete:
            temp_scipt_statement_object.delete()
            # TODO 这里会导致脚本中记录的语句个数和实际的不符，但是无所谓，创建工单的时候重新计算
            # TODO 以工单scripts记录的信息为准
            self.resp_created(msg="sql已删除。")
        else:
            sql_text = params.pop("sql_text")
            temp_scipt_statement_object.from_dict(params)
            temp_scipt_statement_object.parse_single_statement(sql_text)
            temp_scipt_statement_object.save()
            self.resp_created(temp_scipt_statement_object.to_dict())

    patch.argument = {
        "json": {
            "statement_id": "5eda0cc6e88af5ef7e637bd4",
            "sql_text": "select * from t_cmdb;",
            "//comment": "备注",
            "//delete": False
        }
    }

    def post(self):
        """上传多个sql脚本
        目前在apidoc不能测试该接口（上传文件）
        """

        if not len(self.request.files) or not self.request.files.get("file"):
            return self.resp_bad_req(msg="未选择文件。")

        params = self.get_query_args(Schema({
            scm_optional("filter_sql_type", default=None):
                And(scm_str, scm_one_of_choices(parsed_sql.const.ALL_SQL_TYPE)),
        }))
        file_objects = self.request.files.get("file")
        filter_sql_type = params.pop("filter_sql_type")

        scripts = []
        temp_script_statements: [TempScriptStatement] = []
        for file_object in file_objects:
            body = file_object["body"]
            filename = file_object["filename"]
            if not body:
                continue
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
            script_object = TicketScript(script_name=filename)
            scripts.append(script_object)
            temp_script_statements_to_this_script = TempScriptStatement.parse_script(
                body, script_object, filter_sql_type=filter_sql_type)
            script_object.sub_ticket_count = len(temp_script_statements_to_this_script)
            if script_object.sub_ticket_count:
                temp_script_statements += temp_script_statements_to_this_script
        if not temp_script_statements:
            return self.resp_bad_req(msg="请正确选择过滤和脚本sql之间的关系")
        TempScriptStatement.insert(temp_script_statements)
        self.resp_created({
            "scripts": [
                {
                    "script_id": a_script.script_id,
                    "script_name": a_script.script_name
                } for a_script in scripts
            ]
        })

    post.argument = {
        "json": {
            "//filter_sql_type": "DDL"
        }
    }
