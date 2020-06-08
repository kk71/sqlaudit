# Author: kk.Fang(fkfkbill@gmail.com)

import settings
from os import path
from collections import defaultdict

from auth.const import PRIVILEGE
from ticket import const
from .base import *
from utils.schema_utils import *
from utils.datetime_utils import *
from ..sub_ticket import SubTicket
from ..ticket import Ticket
from cmdb.cmdb import *
from models.sqlalchemy import *
from restful_api.modules import *
from ..tasks import TicketExport


class ArchiveHandler(TicketReq):

    def get(self):
        """(DEPRECATED)线下工单的外层归档列表，按照日期，工单类型，审核结果来归档"""

        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE)

        params = self.get_query_args(Schema({
            scm_optional("status", default=None): self.scm_status,
            **self.gen_date(date_start=True, date_end=True),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        date_start, date_end = self.pop_date(params)
        ticket_status = params.pop("status")

        filtered_tickets = Ticket.filter(
            create_time__gte=date_start, create_time__lt=date_end
        ).order_by("-create_time")
        if ticket_status:
            filtered_tickets = filtered_tickets.filter(status=ticket_status)
        filtered_tickets = self.privilege_filter_ticket(filtered_tickets)
        tickets: list = []
        for ticket in filtered_tickets:
            sub_tickets = SubTicket.filter(ticket_id=str(ticket.ticket_id))
            ret_item = {
                **ticket.to_dict(
                    iter_by=lambda k, v: d_to_str(v) if k == "create_time" else v),
                "result_stats": {
                    "static_problem_num": sum([
                        len(x.static) for x in sub_tickets if x]),
                    "dynamic_problem_num": sum([
                        len(x.dynamic) for x in sub_tickets if x])
                }
            }
            tickets.append(ret_item)

        # ds: date: work_list_status  {"k":{"k":[]}}
        ds = defaultdict(lambda: defaultdict(list))
        for ticket in tickets:
            submit_date: str = ticket["create_time"]  # 只有日期没有时间
            ticket_status: int = ticket["status"]
            ds[submit_date][ticket_status].append(ticket)
        rst = []
        for the_date, date_vs in ds.items():
            for ticket_status, tickets in date_vs.items():
                stats = {
                    "create_time": the_date,
                    "status": ticket_status,
                    "num": len(tickets),
                    "result_stats": {
                        'static_problem_num': 0,
                        'dynamic_problem_num': 0
                    }
                }
                for ticket in tickets:
                    stats["result_stats"]["static_problem_num"] += \
                        ticket["result_stats"]["static_problem_num"]
                    stats["result_stats"]["dynamic_problem_num"] += \
                        ticket["result_stats"]["dynamic_problem_num"]
                rst.append(stats)
        rr = sorted(
            rst,
            key=lambda x: arrow.get(x['create_time']).date(),
            reverse=True
        )
        items, p = self.paginate(rr, **p)
        self.resp(items, **p)


class TicketHandler(TicketReq):

    def get(self):
        """工单列表"""

        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE)

        params = self.get_query_args(Schema({
            scm_optional("status"): self.scm_status,
            scm_optional("keyword", default=None): scm_str,
            scm_optional("ticket_id", default=None): scm_str,
            **self.gen_date(),
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        ticket_id = params.pop("ticket_id")
        date_start, date_end = self.pop_date(params)
        p = self.pop_p(params)

        ticket_q = Ticket.filter(**params).order_by("-create_time")
        if keyword:
            ticket_q = self.query_keyword(ticket_q, keyword,
                                          "ticket_id",
                                          "cmdb_id",
                                          "task_name",
                                          "submit_owner",
                                          "audit_owner",
                                          "audit_comment")
        if date_start:
            ticket_q = ticket_q.filter(create_time__gte=date_start)
        if date_end:
            ticket_q = ticket_q.filter(create_time__lt=date_end)
        if ticket_id:
            ticket_q = ticket_q.filter(ticket_id=ticket_id)
        ticket_q = self.privilege_filter_ticket(ticket_q)
        filtered_tickets, p = self.paginate(ticket_q, **p)
        ret = []
        with make_session() as session:
            cmdb_id_connect_name_pairs = dict(session.query(
                CMDB.cmdb_id, CMDB.connect_name))
        for the_ticket in filtered_tickets:
            sub_tickets_q_to_current_ticket = SubTicket.filter(
                ticket_id=str(the_ticket.ticket_id))
            ret_item = {
                **the_ticket.to_dict(),
                "database_name": cmdb_id_connect_name_pairs.get(the_ticket.cmdb_id, None),
                "result_stats": {
                    "static_problem_num": sum([
                        len(x.static) for x in sub_tickets_q_to_current_ticket if x]),
                    "dynamic_problem_num": sum([
                        len(x.dynamic) for x in sub_tickets_q_to_current_ticket if x])
                }
            }
            ret.append(ret_item)

        self.resp(ret, **p)

    get.argument = {
        "querystring": {
            "//status": 1,
            "//keyword": "",
            "//ticket_id": "",
            "//date_start": "",
            "//date_end": "",
            "//page": 1,
            "//per_page": 10
        }
    }

    def post(self):
        """提交工单"""
        raise NotImplementedError

    def patch(self):
        """编辑工单(通过拒绝，评价)"""

        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL)

        params = self.get_json_args(Schema({
            "ticket_id": scm_str,

            scm_optional("audit_comments"): scm_str,
            "status": self.scm_status
        }))
        params["audit_date"] = datetime.now()
        params["audit_owner"] = self.current_user
        ticket_id = params.pop("ticket_id")

        Ticket.filter(ticket_id=ticket_id).update(**{
            "set__" + k: v for k, v in params.items()})
        # TODO timing_send_work_list_status.delay(ticket.to_dict())
        return self.resp_created(msg="更新成功")

    patch.argument = {
        "json": {
            "ticket_id": "",
            "//audit_comments": "",
            "status": 2
        }
    }

    def delete(self):
        """删除工单"""

        params = self.get_json_args(Schema({
            "ticket_id": scm_str
        }))
        ticket_id = params.pop("ticket_id")
        del params

        ticket = Ticket.filter(ticket_id=ticket_id).first()
        ticket.delete()
        SubTicket.filter(ticket_id=ticket_id).delete()
        self.resp(msg="已删除")

    delete.argument = {
        "json": {
            "ticket_id": ""
        }
    }


@as_view("export", group="ticket")
class TicketExportHandler(TicketReq):

    async def get(self):
        """导出工单"""

        params = self.get_query_args(Schema({
            "ticket_id": scm_unempty_str
        }))
        ticket_id = params.pop("ticket_id")

        # 首先获取主工单的基本信息
        the_ticket = Ticket.filter(ticket_id=ticket_id).first()
        work_list = the_ticket.to_dict(
            iter_if=lambda k, v: k not in ("scripts",),
            iter_by=lambda k, v:
                const.ALL_TICKET_STATUS_CHINESE[the_ticket.status]
                if k == "status"
                else v
        )

        # 主要信息
        work_list_heads = [
            "工单ID", "工单类型", "CMDBID", "用户名", "任务名称", "业务系统名称",
            "数据库名称", "SQL数量", "提交时间", "提交人", "审核时间",
            "工单状态", "审核人", "审核意见", "上线时间", "工单的分数"
        ]
        work_list_data = list(work_list.values())
        params_dict = {
            'work_list_heads': work_list_heads,
            'work_list_data': work_list_data
        }

        filename = '_'.join([
            '工单信息',
            work_list['task_name'],
            d_to_str(arrow.now())]) + '.xlsx'

        # 根据工单获得一些统计信息
        work_sub_list = SubTicket.filter(ticket_id=ticket_id).all()
        work_sub_list = [x.to_dict(
            iter_by=lambda k, v: dt_to_str(v)
            if k == 'check_time' else v)
            for x in work_sub_list]
        sql_count = len(work_sub_list)
        fail_count = len([
            x['static'] or x['dynamic']
            for x in work_sub_list
            if x['static'] or x['dynamic']
        ])

        # 静态错误的工单
        static_fail_works = [x for x in work_sub_list if x['static']]
        static_fail_count = len(static_fail_works)

        # 动态错误的工单
        dynamic_fail_works = [x for x in work_sub_list if x['dynamic']]
        dynamic_fail_count = len(dynamic_fail_works)

        fail_heads = ['总脚本数', '失败脚本数', '静态失败数', '动态失败数']
        fail_data = [sql_count, fail_count, static_fail_count, dynamic_fail_count]

        params_dict.update(
            {
                'fail_heads': fail_heads,
                'fail_data': fail_data
            }
        )

        # 获得静态错误的子工单
        static_fail_heads = ['SQL_ID', 'SQL文本', '静态检测结果']
        static_fail_data = [[static_fail_work['statement_id'], static_fail_work['sql_text'],
                             "\n".join([static['rule_name'] for static in static_fail_work['static']])]
                            for static_fail_work in static_fail_works]
        params_dict.update(
            {
                'static_fail_heads': static_fail_heads,
                'static_fail_data': static_fail_data
            }
        )
        # 获得动态错误的子工单
        dynamic_fail_heads = ['SQL_ID', 'SQL文本', '动态检测结果']
        dynamic_fail_data = [[dynamic_fail_work['statement_id'], dynamic_fail_work['sql_text'],
                              "\n".join([dynamic['rule_name'] for dynamic in dynamic_fail_work['dynamic']])]
                             for dynamic_fail_work in dynamic_fail_works]

        params_dict.update(
            {
                'dynamic_fail_heads': dynamic_fail_heads,
                'dynamic_fail_data': dynamic_fail_data
            }
        )

        # 获得所有子工单
        all_work_heads = ['SQL_ID', 'SQL文本', '静态检测结果', '动态检测结果']
        all_work_data = [[x['statement_id'], x['sql_text'],
                          "\n".join([y['rule_name'] for y in x['static']]),
                          "\n".join([y['rule_name'] for y in x['dynamic']])]
                         for x in work_sub_list]

        params_dict.update(
            {
                'all_work_heads': all_work_heads,
                'all_work_data': all_work_data
            }
        )
        await TicketExport.async_shoot(filename=filename, parame_dict=params_dict)
        await self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})

    get.argument = {
        "querystring": {
            "ticket_id": ""
        }
    }
