# Author: kk.Fang(fkfkbill@gmail.com)

from os import path

import xlsxwriter
from mongoengine import Q

import settings
from utils.datetime_utils import *
from utils.schema_utils import *
from .base import *
from .. import const
from ..sub_ticket import SubTicket
from ..ticket import Ticket


class SubTicketHandler(TicketReq):

    def filter_sub_ticket(self):
        params = self.get_query_args(Schema({
            scm_optional("error_type", default=None): scm_one_of_choices(
                const.ALL_SUB_TICKET_FILTERS),
            scm_optional("start_time", default=None): scm_date,
            scm_optional("end_time", default=None): scm_date_end,
            scm_optional("ticket_id"): scm_unempty_str,
            scm_optional("schema_name", default=None): scm_str,
            scm_optional("cmdb_id", default=None): scm_int,
            scm_optional("keyword", default=None): scm_str,
        }, ignore_extra_keys=True))
        error_type = params.pop("error_type")
        keyword = params.pop("keyword")
        start_time, end_time = params.pop("start_time"), params.pop("end_time")
        schema_name = params.pop("schema_name")
        cmdb_id = params.pop("cmdb_id")

        to_filter = {k: v for k, v in params.items() if k in
                     ("ticket_id",)}

        # TODO 需要根据权限加过滤判断
        q = SubTicket.objects(**to_filter)
        if cmdb_id:
            ticket_id_list = [
                i[0]
                for i in Ticket.objects().filter_by(
                    cmdb_id=cmdb_id).values_list("ticket_id").all()
            ]
            q = q.filter(ticket_id__in=ticket_id_list)
        if start_time:
            q = q.filter(create_time__gt=start_time)
        if end_time:
            q = q.filter(create_time__lt=end_time)
        if keyword:
            q = self.query_keyword(q, keyword, "static", "dynamic", "comments")
        if schema_name:
            ticket_id_in_tuple = Ticket.objects(). \
                filter_by(schema_name=schema_name). \
                values_list("ticket_id").all()
            ticket_id_list = [i[0] for i in ticket_id_in_tuple]
            q = q.filter(ticket_id__in=ticket_id_list)
        if error_type == const.SUB_TICKET_WITH_STATIC_PROBLEM:
            q = q.filter(static__not__size=0)
        elif error_type == const.SUB_TICKET_WITH_DYNAMIC_PROBLEM:
            q = q.filter(dynamic__not__size=0)
        elif error_type == const.SUB_TICKET_HAS_FAILURE:
            # 仅返回分析失败的子工单，即error_msg不为空的子工单
            q = q.filter(error_msg__nin=["", None])
        elif error_type == const.SUB_TICKET_ALL_WITH_PROBLEM:
            # 这个类型包含上面三种情况
            q = q.filter(Q(static__not__size=0) |
                         Q(dynamic__not__size=0) |
                         Q(error_msg__nin=[None, ""]))
        elif error_type is None or error_type == const.SUB_TICKET_ALL:
            pass  # reserved but should be useless
        else:
            assert 0
        q = self.privilege_filter_sub_ticket(q)
        q = q.order_by("position")
        return q

    def get(self):
        """子工单列表"""
        params = self.get_query_args(Schema({
            **self.gen_p(),
        }, ignore_extra_keys=True))
        p = self.pop_p(params)

        q = self.filter_sub_ticket()
        items, p = self.paginate(q, **p)
        # 加上工单的task_name
        ticket_ids: list = list({i.ticket_id for i in items})
        ticket_id_pairs = Ticket.objects(). \
            filter(ticket_id__in=ticket_ids). \
            values_list("ticket_id", "task_name")
        ticket_id_pair_dict = dict(ticket_id_pairs)

        self.resp([
            {
                **i.to_dict(),
                "task_name": ticket_id_pair_dict.get(i.ticket_id, None)
            } for i in items
        ], **p)

    def patch(self):
        """编辑单个子工单"""
        params = self.get_json_args(Schema({
            "statement_id": scm_unempty_str,

            scm_optional("sql_text"): scm_unempty_str,
            scm_optional("comments"): scm_str,
        }))
        statement_id = params.pop("statement_id")

        sub_ticket = SubTicket.objects(statement_id=statement_id).first()
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

        sub_ticket = SubTicket.objects(statement_id=statement_id).first()
        if not sub_ticket:
            return self.resp_bad_req(msg=f"找不到子工单编号为{statement_id}")
        if SubTicket.objects(ticket_id=sub_ticket.ticket_id).count() == 1:
            return self.resp_bad_req(msg="删除失败，工单应该有至少一条语句。")
        sub_ticket.delete()
        self.resp_created(msg="删除成功。")


class SubTicketExportHandler(SubTicketHandler):

    def get(self):
        """导出子工单"""
        params = self.get_query_args(Schema({
            "export_type": scm_one_of_choices(["all_filtered", "selected"]),

            scm_optional(object): object
        }))
        export_type = params.pop("export_type")
        if export_type == "all_filtered":
            # 导出全部子工单按照条件过滤出来的结果
            q = self.filter_sub_ticket()
        elif export_type == "selected":
            # 给出子工单id，仅导出这些。
            params = self.get_query_args(Schema({
                "statement_id_list": scm_dot_split_str,
                scm_optional(object): object
            }))
            statement_id_list = params.pop("statement_id_list")
            q = SubTicket.objects(statement_id__in=statement_id_list)
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
        fields = ["工单编号", "SQL文本", "静态检测结果", "动态检测结果", "上线状态", "错误信息"]
        for x, field in enumerate(fields):
            ws.write(0, x, field.upper(), format_title)
        for row_num, sub_ticket in enumerate(q.all()):
            row_num += 1
            ws.write(row_num, 0, sub_ticket.task_name, format_text)
            ws.write(row_num, 1, sub_ticket.sql_text, format_text)
            ws.write(row_num, 2, "\n".join(
                [x['rule_desc'] for x in sub_ticket.to_dict()['static']]),
                     format_text)
            ws.write(row_num, 3, "\n".join(
                [x["rule_desc"] for x in sub_ticket.to_dict()['dynamic']]),
                     format_text)
            ws.write(row_num, 4, sub_ticket.online_status, format_text)
            ws.write(row_num, 5, sub_ticket.error_msg, format_text)
        wb.close()
        self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})
