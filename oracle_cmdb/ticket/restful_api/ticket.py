# Author: kk.Fang(fkfkbill@gmail.com)

import chardet

from utils.schema_utils import *

import ticket.restful_api.ticket


class SQLUploadHandler(ticket.restful_api.ticket.SQLUploadHandler):

    def post(self):
        """上传一个sql脚本"""
        if not len(self.request.files) or not self.request.files.get("file"):
            return self.resp_bad_req(msg="未选择文件。")

        params = self.get_query_args(Schema({
            scm_optional("filter_sql_type", default=None):
                And(scm_int, scm_one_of_choices(ALL_SQL_TYPE)),
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

        parsed_sql_obj = ParsedSQL(body)
        session_id = uuid.uuid4().hex
        with make_session() as session:
            to_add = []
            for i, obj in enumerate(parsed_sql_obj):
                if filter_sql_type is not None and \
                        obj.sql_type == filter_sql_type:
                    continue
                if not obj.normalized or not obj.normalized_without_comment:
                    continue
                wlat = WorkListAnalyseTemp(
                    session_id=session_id,
                    sql_text=obj.normalized,
                    sql_text_no_comment=obj.normalized_without_comment,
                    comments="",
                    sql_type=obj.sql_type,
                    num=i
                )
                to_add.append(wlat)
            if not to_add:
                return self.resp_bad_req(msg="所传SQL脚本不包含任何SQL")
            TicketMeta(
                session_id=session_id,
                original_sql=parsed_sql_obj.get_original_sql(),
                comment_striped_sql=parsed_sql_obj.get_comment_striped_sql()
            ).save()
            session.add_all(to_add)
        self.resp_created({"session_id": session_id})
