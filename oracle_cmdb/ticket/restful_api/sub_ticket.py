# Author: kk.Fang(fkfkbill@gmail.com)

from prettytable import PrettyTable

from utils.schema_utils import *

from ticket.restful_api.base import *


class SQLPlanHandler(TicketReq):

    def get(self):
        """获取子工单单条sql语句的执行计划"""
        params = self.get_query_args(Schema({
            "statement_id": scm_unempty_str,
            scm_optional("plan_id"): scm_gt0_int
        }))
        # 指明的表中列明以及对应列数据在mongo-engine里的字段名
        sql_plan_head = {
            'Id': "the_id",
            'Operation': "operation_display_with_options",
            'Name': "object_name",
            'Rows': "cardinality",
            'Bytes': "bytes",
            'Cost (%CPU)': "cost",
            'Time': "time"
        }

        pt = PrettyTable(sql_plan_head.keys())
        pt.align = "l"  # 左对齐
        sql_plans = OracleTicketSQLPlan. \
            objects(**params). \
            order_by("plan_id", "the_id"). \
            values_list(*sql_plan_head.values())
        for sql_plan in sql_plans:
            to_add = [i if i is not None else " " for i in sql_plan]
            m, s = divmod(to_add[-1] if to_add[-1] else 0, 60)
            h, m = divmod(m, 60)
            to_add[-1] = "%02d:%02d:%02d" % (h, m, s)
            if 8 > len(str(to_add[3])) > 5:
                to_add[3] = str(round(to_add[3] // 1024)) + "K"
                if len(str(to_add[3])) >= 8:
                    to_add[3] = str(round(to_add[3] // 1024 // 1024)) + "M"
            if 8 > len(str(to_add[4])) > 5:
                to_add[4] = str(round(to_add[4] // 1024)) + "K"
                if len(str(to_add[4])) >= 8:
                    to_add[4] = str(round(to_add[4] // 1024 // 1024)) + "M"
            if 8 > len(str(to_add[5])) > 5:
                to_add[5] = str(round(to_add[5] // 1024)) + "K"
                if len(str(to_add[5])) >= 8:
                    to_add[5] = str(round(to_add[5] // 1024 // 1024)) + "M"
            pt.add_row(to_add)

        output_table = str(pt)
        self.resp({
            'sql_plan_text': output_table,
        })


class SubTicketIssueHandler(TicketReq):

    def patch(self):
        """修改子工单内的规则，修改后重新计算工单的分数"""
        db_type, params = self.alternative_args_db_type(
            func=self.get_json_args,
            oracle=Schema({
                "work_list_id": scm_gt0_int,
                "statement_id": scm_unempty_str,
                "ticket_rule_name": scm_unempty_str,
                "analyse_type": scm_one_of_choices(ALL_TICKET_ANALYSE_TYPE),
                scm_optional("action", default="delete"):
                    scm_one_of_choices(["update", "delete"]),
                scm_optional("update"): {
                    "minus_score": scm_num
                }
            })
        )

        if db_type == DB_ORACLE:
            work_list_id = params.pop("work_list_id")
            statement_id = params.pop("statement_id")
            ticket_rule_name = params.pop("ticket_rule_name")
            analyse_type = params.pop("analyse_type")
            action = params.pop("action")
            sub_ticket = OracleTicketSubResult.objects(
                work_list_id=work_list_id, statement_id=statement_id).first()
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
            with make_session() as session:
                ticket = session.query(WorkList). \
                    filter(WorkList.work_list_id == work_list_id).first()
                ticket.calc_score()  # 修改了之后重新计算整个工单的分数
                session.add(ticket)
            self.resp_created(sub_ticket.to_dict())
        else:
            self.resp_bad_req(msg="数据库类型错误")
