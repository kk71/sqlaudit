# Author: kk.Fang(fkfkbill@gmail.com)

import ticket.const
from restful_api.modules import *
from utils.schema_utils import *
from ticket.restful_api.base import *
from ..sql_plan import OracleTicketSQLPlan
from ..sub_ticket import OracleSubTicket
from ..ticket import OracleTicket


@as_view("sql_plan", group="ticket")
class SQLPlanHandler(TicketReq):

    def get(self):
        """获取子工单单条sql语句的执行计划"""

        params = self.get_query_args(Schema({
            "statement_id": scm_unempty_str,
            scm_optional("plan_id"): scm_gt0_int
        }))
        self.resp({
            'sql_plan_text': OracleTicketSQLPlan.sql_plan_table(**params),
        })

    get.argument = {
        "querystring": {
            "statement_id": "juINDpGfSN2t3ukow7SoSw==",
            "plan_id": ""
        }
    }


@as_view("issue", group="ticket")
class SubTicketIssueHandler(TicketReq):

    def patch(self):
        """删除子工单内的issue，修改后重新计算工单的分数"""

        params = self.get_json_args(Schema({
            "statement_id": scm_unempty_str,
            "ticket_rule_name": scm_unempty_str,
            "analyse_type": scm_one_of_choices(
                ticket.const.ALL_TICKET_ANALYSE_TYPE),
            scm_optional("action", default="delete"):
                scm_one_of_choices(["update", "delete"]),
            scm_optional("update"): {
                "minus_score": scm_num
            }
        }))

        statement_id = params.pop("statement_id")
        ticket_rule_name = params.pop("ticket_rule_name")
        analyse_type = params.pop("analyse_type")
        action = params.pop("action")

        sub_ticket = OracleSubTicket.filter(statement_id=statement_id).first()
        embedded_list = getattr(sub_ticket, analyse_type.lower())
        operated = False
        for n, sub_ticket_item in enumerate(embedded_list):
            if sub_ticket_item.rule_name == ticket_rule_name:
                if action == "delete":
                    del embedded_list[n]  # 目前只支持删除子工单的规则结果
                else:
                    assert 0
                operated = True
                break
        if not operated:
            return self.resp_bad_req(msg="未找到对应需要操作的规则。")
        sub_ticket.save()
        the_ticket = OracleTicket.filter(
            ticket_id=sub_ticket.ticket_id).first()
        the_ticket.calculate_score()  # 修改了之后重新计算整个工单的分数
        the_ticket.save()
        self.resp_created(sub_ticket.to_dict())

    patch.argument = {
        "json": {
        "statement_id": "juINDpGfSN2t3ukow7SoSw==",
        "ticket_rule_name": "SELECT_ANY",
        "analyse_type": "STATIC",
        "//action":"delete",
        "//update": {
            "minus_score": ""}
        }
    }
