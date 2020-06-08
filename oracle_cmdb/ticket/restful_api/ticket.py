# Author: kk.Fang(fkfkbill@gmail.com)

import ticket.restful_api.ticket
from restful_api.modules import *
from models.sqlalchemy import make_session
from utils.schema_utils import *
from ..ticket import OracleTicket
from ...cmdb import *
from .. import task
from ticket.task_name_utils import *
from cmdb.const import DB_ORACLE


@as_view(group="ticket")
class OracleTicketHandler(ticket.restful_api.ticket.TicketHandler):

    def post(self):
        """提交工单"""

        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            scm_optional("schema_name", default=None): scm_unempty_str,
            "audit_role_id": scm_gt0_int,
            scm_optional("task_name", default=None): scm_unempty_str,
            "script_ids": [scm_unempty_str],
            scm_optional("online_username", default=None): scm_str,
            scm_optional("online_password", default=None): scm_str,
        }))
        params["submit_owner"] = self.current_user
        script_ids: list = params.pop("script_ids")

        with make_session() as session:
            the_cmdb = session.query(OracleCMDB).filter(
                OracleCMDB.cmdb_id == params["cmdb_id"]).first()
            if not the_cmdb.check_privilege():
                return self.resp_forbidden(
                    msg=f"当前纳管库的登录用户({the_cmdb.username})权限不足，"
                        "无法做诊断分析。"
                )

            new_ticket = OracleTicket(db_type=DB_ORACLE)
            if not params["schema_name"]:
                # 缺省就用纳管库登录的用户去执行动态审核（也就是explain plan for）
                # 缺省的情况下，假设用户会在自己上传的sql语句里带上表的schema
                # 如果他的sql不带上schema，则它必须在提交工单的时候指定sql运行的schema_name
                # 否则无法确定他的对象是处在哪个schema下面的
                # 默认的纳管库用户是需要打开权限的，以保证能够在访问别的schema的对象
                # 所以需要在前面先验证纳管库登录的用户是否有足够的权限。
                params["schema_name"] = the_cmdb.username
            if not params["task_name"]:
                params['task_name'] = get_available_task_name(
                    submit_owner=params["submit_owner"]
                )
            new_ticket.from_dict(params)
            new_ticket.save()
            task.ticket_analyse.delay(
                ticket_id=str(new_ticket.ticket_id), script_ids=script_ids)

        self.resp_created(msg="已安排分析，请稍后查询分析结果。")

    post.argument = {
        "json": {
            "cmdb_id": "2526",
            "//schema_name": "APEX",
            "audit_role_id": "61",
            "//task_name": "",
            "script_ids": ['701325c6081c4048b95d62d3e6fc29f1'],
            "//online_username": "",
            "//online_password": ""
        }
    }
