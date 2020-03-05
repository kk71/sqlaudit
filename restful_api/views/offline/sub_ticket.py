# Author: kk.Fang(fkfkbill@gmail.com)

import arrow
import xlsxwriter
from os import path
from mongoengine import Q
from schema import Schema, Optional
from prettytable import PrettyTable

import settings
from .base import *
from utils.const import *
from utils.schema_utils import *
from utils.datetime_utils import dt_to_str
from models.oracle import *
from models.mongo.offline_oracle import *


class SubTicketHandler(TicketReq):

    def filter_sub_ticket(self, session):
        params = self.get_query_args(Schema({
            Optional("error_type", default=None): scm_one_of_choices({
                "static",
                "dynamic",
                "all_with_problems",
                "failure",
                "all"
            }),
            Optional("start_time", default=None): scm_date,
            Optional("end_time", default=None): scm_date_end,
            Optional("work_list_id"): scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("cmdb_id", default=None): scm_int,
            Optional("keyword", default=None): scm_str,
        }, ignore_extra_keys=True))
        error_type = params.pop("error_type")
        keyword = params.pop("keyword")
        start_time, end_time = params.pop("start_time"), params.pop("end_time")
        schema_name = params.pop("schema_name")
        cmdb_id = params.pop("cmdb_id")

        to_filter = {k: v for k, v in params.items() if k in
                     ("work_list_id",)}

        # TODO 需要根据权限加过滤判断
        q = TicketSubResult.objects(**to_filter)
        if cmdb_id:
            work_list_id_list = [i[0] for i in session.query(WorkList).
                filter_by(cmdb_id=cmdb_id).
                with_entities(WorkList.work_list_id).all()]
            q = q.filter(work_list_id__in=work_list_id_list)
        if start_time:
            q = q.filter(check_time__gt=start_time)
        if end_time:
            q = q.filter(check_time__lt=end_time)
        if keyword:
            q = self.query_keyword(q, keyword, "static", "dynamic", "comments")
        if schema_name:
            work_lisk_id_in_tuple = session.query(WorkList). \
                filter_by(schema_name=schema_name). \
                with_entities(WorkList.work_list_id).all()
            work_list_id_list = [i[0] for i in work_lisk_id_in_tuple]
            q = q.filter(work_list_id__in=work_list_id_list)
        if error_type == "static":
            q = q.filter(static__not__size=0)
        elif error_type == "dynamic":
            q = q.filter(dynamic__not__size=0)
        elif error_type == "failure":
            # 仅返回分析失败的子工单，即error_msg不为空的子工单
            q = q.filter(error_msg__nin=["", None])
        elif error_type == "all_with_problems":
            # 这个类型包含上面三种情况
            q = q.filter(Q(static__not__size=0) |
                         Q(dynamic__not__size=0) |
                         Q(error_msg__nin=[None, ""]))
        elif error_type is None or error_type == "all":
            pass  # reserved but should be useless
        else:
            assert 0
        q = self.privilege_filter_sub_ticket(q, session)
        q = q.order_by("position")
        return q

    def get(self):
        """子工单列表"""
        params = self.get_query_args(Schema({
            **self.gen_p(),
        }, ignore_extra_keys=True))
        p = self.pop_p(params)

        with make_session() as session:
            q = self.filter_sub_ticket(session)
            items, p = self.paginate(q, **p)
            # 加上工单的task_name
            work_list_ids: list = list({i.work_list_id for i in items})
            work_list_id_pairs = session.query(
                WorkList.work_list_id,
                WorkList.task_name
            ).filter(WorkList.work_list_id.in_(work_list_ids))
            work_list_id_pair_dict = dict(work_list_id_pairs)

            self.resp([
                {
                    **i.to_dict(),
                    "task_name": work_list_id_pair_dict.get(i.work_list_id, None)
                } for i in items], **p)

    def patch(self):
        """编辑单个子工单"""
        params = self.get_json_args(Schema({
            "statement_id": scm_unempty_str,

            Optional("sql_text"): scm_unempty_str,
            Optional("comments"): scm_str,
        }))
        statement_id = params.pop("statement_id")

        sub_ticket = TicketSubResult.objects(statement_id=statement_id).first()
        if not sub_ticket:
            return self.resp_bad_req(msg=f"找不到子工单编号为{statement_id}")
        sub_ticket.from_dict(params)
        sub_ticket.save()
        self.resp_created(sub_ticket.to_dict())

    def delete(self):
        """删除子工单"""
        params = self.get_json_args(Schema({
            "statement_id": scm_unempty_str
        }))
        statement_id = params.pop("statement_id")

        sub_ticket = TicketSubResult.objects(statement_id=statement_id).first()
        if not sub_ticket:
            return self.resp_bad_req(msg=f"找不到子工单编号为{statement_id}")
        if TicketSubResult.objects(work_list_id=sub_ticket.work_list_id).count() == 1:
            return self.resp_forbidden(msg="删除失败，工单应该有至少一条语句。")
        sub_ticket.delete()
        self.resp_created(msg="删除成功。")


class SubTicketExportHandler(SubTicketHandler):

    def get(self):
        """导出子工单"""
        params = self.get_query_args(Schema({
            "export_type": scm_one_of_choices(["all_filtered", "selected"]),

            Optional(object): object
        }))
        export_type = params.pop("export_type")
        with make_session() as session:
            if export_type == "all_filtered":
                # 导出全部子工单按照条件过滤出来的结果
                q = self.filter_sub_ticket(session)
            elif export_type == "selected":
                # 给出子工单id，仅导出这些。
                params = self.get_query_args(Schema({
                    "statement_id_list": scm_dot_split_str,
                    Optional(object): object
                }))
                statement_id_list = params.pop("statement_id_list")
                q = TicketSubResult.objects(statement_id__in=statement_id_list)
            else:
                assert 0

            filename = f"export_sub_ticket_{arrow.now().timestamp}.xlsx"
            full_filename = path.join(settings.EXPORT_DIR, filename)
            wb = xlsxwriter.Workbook(full_filename)
            ws = wb.add_worksheet('子工单报告')
            ws.set_column(0, 2, 20)
            ws.set_column(3, 4, 18)
            ws.set_column(5, 5, 10)
            ws.set_column(6, 6, 50)
            ws.set_row(0, 30)
            format_title = wb.add_format({
                'bold': 1,
                'size': 14,
                'align': 'center',
                'valign': 'vcenter',

            })
            format_text = wb.add_format({
                'align': 'left',
                'valign': 'vcenter',
                'text_wrap': True,
            })
            fields = ["主工单ID", "SQL_ID", "SQL文本", "静态检测结果", "动态检测结果",
                      "检测时间", "执行时长(ms)", "错误信息", "备注"]
            for x, field in enumerate(fields):
                ws.write(0, x, field.upper(), format_title)
            for row_num, sub_ticket in enumerate(q.all()):
                row_num += 1
                ws.write(row_num, 0, sub_ticket.work_list_id, format_text)
                ws.write(row_num, 1, sub_ticket.statement_id, format_text)
                ws.write(row_num, 2, sub_ticket.sql_text, format_text)
                ws.write(row_num, 3, "\n".join(
                    [x['rule_name'] for x in sub_ticket.to_dict()['static']]),
                         format_text)
                ws.write(row_num, 4, "\n".join(
                    [x["rule_name"] for x in sub_ticket.to_dict()['dynamic']]),
                         format_text)
                ws.write(row_num, 5, dt_to_str(sub_ticket.check_time))
                # ws.write(row_num, 6, sub_ticket.check_owner, format_text)
                # ws.write(row_num, 7, dt_to_str(sub_ticket.online_date))
                # ws.write(row_num, 8, sub_ticket.online_owner, format_text)
                ws.write(row_num, 6, sub_ticket.elapsed_seconds, format_text)
                # ws.write(row_num, 10, sub_ticket.status, format_text)
                ws.write(row_num, 7, sub_ticket.error_msg, format_text)
                ws.write(row_num, 8, sub_ticket.comments, format_text)
            wb.close()
            self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})


class SQLPlanHandler(TicketReq):

    def get(self):
        """获取子工单单条sql语句的执行计划"""
        db_type, params = self.alternative_args_db_type(
            self.get_query_args,
            oracle=Schema({
                "statement_id": scm_unempty_str,
                scm_optional("plan_id"): scm_gt0_int
            })
        )
        if db_type == DB_ORACLE:
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
            sql_plans = OracleTicketSQLPlan.\
                objects(**params).\
                order_by("plan_id", "the_id").\
                values_list(*sql_plan_head.values())
            for sql_plan in sql_plans:
                to_add = [i if i is not None else " " for i in sql_plan]
                if not isinstance(to_add[-1], str):
                    to_add[-1] = arrow.get(to_add[-1]).time().strftime("%H:%M:%S")
                pt.add_row(to_add)

            output_table = str(pt)
            self.resp({
                'sql_plan_text': output_table,
            })
        else:
            self.resp_bad_req(msg="数据库类型错误")


class SubTicketRuleHandler(TicketReq):

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
                ticket = session.query(WorkList).\
                    filter(WorkList.work_list_id == work_list_id).first()
                ticket.calc_score()  # 修改了之后重新计算整个工单的分数
                session.add(ticket)
            self.resp_created(sub_ticket.to_dict())
        else:
            self.resp_bad_req(msg="数据库类型错误")
