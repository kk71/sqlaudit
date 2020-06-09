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


@as_view("archive", group="ticket")
class ArchiveHandler(TicketReq):

    def get(self):
        """线下工单的外层归档列表，
        按照日期，工单状态归档，
        展现工单数量，动静态问题数量
        """
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

    get.argument = {
        "querystring": {
            "//status": "2",
            "date_start": "2020-05-25",
            "date_end": "2020-06-07",
            "page": "1",
            "per_page": "10"
        }
    }


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
            "//status": "2",
            "//keyword": "",
            "//ticket_id": "5edcc7d1568d8355c5dab195",
            "date_start": "2020-06-07",
            "date_end": "2020-06-07",
            "page": "1",
            "per_page": "10"
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
            "ticket_id": "5edcc7d1568d8355c5dab195",
            "//audit_comments": "",
            "status": "3"
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
            "ticket_id": "5edcc4ed3c0f3c30fcf3d91a"
        }
    }


@as_view("export", group="ticket")
class TicketExportHandler(TicketReq):

    async def get(self):
        """工单导出"""
        params = self.get_query_args(Schema({
            "ticket_id": scm_unempty_str
        }))
        ticket_id = params.pop("ticket_id")

        # 获取工单数据
        ticket_heads = [
            "工单ID", "任务名称", "数据库类型", "CMDBID", "Schema",
            "脚本数量", "SQL数量", "工单的分数", "提交时间", "提交人",
            "工单状态", "审核时间", "审核人", "审核意见"]

        the_ticket = Ticket.filter(ticket_id=ticket_id).first()
        the_ticket = the_ticket.to_dict(
            iter_by=lambda k, v:
            const.ALL_TICKET_STATUS_CHINESE[the_ticket.status]
            if k == "status"
            else v,
        )
        the_ticket['script_sum']=len(the_ticket.pop("scripts"))

        params_dict = {
            'ticket_heads': ticket_heads,
            'the_ticket': the_ticket
        }

        #导出工单文件名
        filename = '_'.join([
            '工单信息',
            the_ticket['task_name'],
            d_to_str(arrow.now())]) + '.xlsx'

        # 获取子工单数量统计
        sub_ticket_stats_heads = ["总sql数量", "问题sql数量", "静态检测结果数", "动态检测结果数"]
        sub_ticket_list = SubTicket.filter(ticket_id=ticket_id).all()
        sub_ticket_list = [x.to_dict(
            iter_by=lambda k, v: dt_to_str(v)
            if k == 'check_time' else v)
            for x in sub_ticket_list]

        sql_count = len(sub_ticket_list)
        issue_sql_count = len([
            True for x in sub_ticket_list
            if x['static'] or x['dynamic']])
        issue_static = [x for x in sub_ticket_list if x['static']]
        issue_static_count = len(issue_static)
        issue_dynamic = [x for x in sub_ticket_list if x['dynamic']]
        issue_dynamic_count = len(issue_dynamic)
        sub_ticket_stats = [sql_count, issue_sql_count,issue_static_count, issue_dynamic_count]
        params_dict.update(
            {
                'sub_ticket_stats_heads': sub_ticket_stats_heads,
                'sub_ticket_stats': sub_ticket_stats
            }
        )

        # 获得静态问题子工单
        issue_static_sub_ticket_heads = ['序号','任务名称','脚本名称','SQL文本','静态检测结果','错误信息']
        issue_static_sub_ticket = [[a,x['task_name'],x['script']['script_name'],
                                    x['sql_text'],"\n".join([y['rule_desc'] for y in x['static']]),
                                    x['error_msg']] for a,x in enumerate(issue_static)]
        params_dict.update(
            {
                'issue_static_sub_ticket_heads': issue_static_sub_ticket_heads,
                'issue_static_sub_ticket': issue_static_sub_ticket
            }
        )

        # 获得动态问题子工单
        issue_dynamic_sub_ticket_heads = ['序号', '任务名称', '脚本名称', 'SQL文本', '动态检测结果', '错误信息']
        issue_dynamic_sub_ticket= [[a,x['task_name'],x['script']['script_name'],
                                    x['sql_text'],"\n".join([y['rule_desc'] for y in x['dynamic']]),
                                    x['error_msg']] for a,x in enumerate(issue_dynamic)]

        params_dict.update(
            {
                'issue_dynamic_sub_ticket_heads': issue_dynamic_sub_ticket_heads,
                'issue_dynamic_sub_ticket': issue_dynamic_sub_ticket
            }
        )

        # 获得所有动静态问题子工单
        all_issue_sub_ticket_heads = ['序号', '任务名称', '脚本名称', 'SQL文本', '静态检测结果','动态检测结果', '错误信息']
        all_issue_sub_ticket =[[a,x['task_name'],x['script']['script_name'],x['sql_text'],
                               "\n".join([y['rule_desc'] for y in x['static']]),
                                "\n".join([y['rule_desc'] for y in x['dynamic']]),
                                x['error_msg']] for a,x in enumerate(sub_ticket_list)]
        params_dict.update(
            {
                'all_issue_sub_ticket_heads': all_issue_sub_ticket_heads,
                'all_issue_sub_ticket': all_issue_sub_ticket
            }
        )
        await TicketExport.async_shoot(filename=filename, parame_dict=params_dict)
        await self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})

    get.argument = {
        "querystring": {
            "ticket_id": "5edef7e22a0c111df052f3b7"
        }
    }
