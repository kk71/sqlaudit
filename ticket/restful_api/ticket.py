# Author: kk.Fang(fkfkbill@gmail.com)

import settings
from os import path
from collections import defaultdict

from ticket.const import ALL_TICKET_STATUS
from .base import *
from utils.const import *
from utils.schema_utils import *
from utils.datetime_utils import *
from utils.conc_utils import AsyncTimeout
from ..sub_ticket import SubTicket


class ArchiveHandler(TicketReq):

    def get(self):
        """线下工单的外层归档列表，按照日期，工单类型，审核结果来归档"""
        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE)

        params = self.get_query_args(Schema({
            "date_start": scm_date,
            "date_end": scm_date_end,
            scm_optional("status", default=None): And(
                scm_int, scm_one_of_choices(ALL_TICKET_STATUS)),
            **self.gen_p()
        }))
        p = self.pop_p(params)
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        work_list_status = params.pop("work_list_status")

        with make_session() as session:
            filtered_tickets = session.query(WorkList).filter(WorkList.submit_date >= date_start,
                                                              WorkList.submit_date < date_end). \
                order_by(WorkList.work_list_id.desc())
            if work_list_status is not None:
                filtered_tickets = filtered_tickets.filter(
                    WorkList.work_list_status == work_list_status)
            filtered_tickets = self.privilege_filter_ticket(filtered_tickets)
            tickets: list = []
            for ticket in filtered_tickets:
                sub_tickets = TicketSubResult.objects(
                    work_list_id=ticket.work_list_id)
                ret_item = {
                    **ticket.to_dict(
                        iter_by=lambda k, v:
                        arrow.get(v).format(const.COMMON_DATE_FORMAT)
                        if k == "submit_date" else v),
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
                submit_date: str = ticket["submit_date"]  # 只有日期没有时间
                work_list_status: int = ticket["work_list_status"]
                ds[submit_date][work_list_status].append(ticket)
            rst = []
            for the_date, date_vs in ds.items():
                for work_list_status, tickets in date_vs.items():
                    stats = {
                        "submit_date": the_date,
                        "work_list_status": work_list_status,
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
                key=lambda x: arrow.get(x['submit_date']).date(),
                reverse=True
            )
            items, p = self.paginate(rr, **p)
            self.resp(items, **p)


class TicketHandler(TicketReq):

    def get(self):
        """工单列表"""

        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE)

        params = self.get_query_args(Schema({
            scm_optional("status", default=None):
                And(scm_int, scm_one_of_choices(ALL_TICKET_STATUS)),
            scm_optional("keyword", default=None): scm_str,
            scm_optional("date_start", default=None): scm_date,
            scm_optional("date_end", default=None): scm_date_end,
            scm_optional("ticket_id", default=None): scm_int,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        work_list_status: int = params.pop("work_list_status")
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")
        work_list_id = params.pop("work_list_id")
        p = self.pop_p(params)

        with make_session() as session:
            q = session.query(WorkList). \
                filter_by(**params). \
                order_by(WorkList.work_list_id.desc())
            if work_list_status is not None:  # take care of the value 0!
                q = q.filter_by(work_list_status=work_list_status)
            if keyword:
                q = self.query_keyword(q, keyword,
                                       WorkList.work_list_id,
                                       WorkList.cmdb_id,
                                       WorkList.schema_name,
                                       WorkList.task_name,
                                       WorkList.system_name,
                                       WorkList.database_name,
                                       WorkList.submit_owner,
                                       WorkList.audit_owner,
                                       WorkList.audit_comments)
            if date_start:
                q = q.filter(WorkList.submit_date >= date_start)
            if date_end:
                q = q.filter(WorkList.submit_date < date_end)
            if work_list_id:
                q = q.filter(WorkList.work_list_id == work_list_id)
            q = self.privilege_filter_ticket(q)
            filtered_tickets, p = self.paginate(q, **p)
            ret = []
            for ticket in filtered_tickets:
                sub_tickets_to_current_ticket = TicketSubResult.objects(
                    work_list_id=ticket.work_list_id)
                ret_item = {
                    **ticket.to_dict(),
                    "result_stats": {
                        "static_problem_num": sum([
                            len(x.static) for x in sub_tickets_to_current_ticket if x]),
                        "dynamic_problem_num": sum([
                            len(x.dynamic) for x in sub_tickets_to_current_ticket if x])
                    },
                    "sql_counts": TicketSubResult.
                        objects(work_list_id=ticket.work_list_id).count()
                }
                ret.append(ret_item)

            self.resp(ret, **p)

    def post(self):
        """提交工单"""
        raise NotImplementedError

    def patch(self):
        """编辑工单状态"""
        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL)

        params = self.get_json_args(Schema({
            "ticket_id": scm_int,
            scm_optional("audit_comment"): scm_str,
            "status": And(scm_int, scm_one_of_choices(ALL_TICKET_STATUS))
        }))
        params["audit_date"] = datetime.now()
        params["audit_owner"] = self.current_user
        work_list_id = params.pop("work_list_id")
        with make_session() as session:
            session.query(WorkList). \
                filter(WorkList.work_list_id == work_list_id). \
                update(params)
            ticket = session.query(WorkList). \
                filter(WorkList.work_list_id == work_list_id). \
                first()
            timing_send_work_list_status.delay(ticket.to_dict())
        return self.resp_created(msg="更新成功")

    def delete(self):
        """删除工单"""
        params = self.get_json_args(Schema({
            "ticket_id": scm_int
        }))
        work_list_id = params.pop("work_list_id")
        del params

        with make_session() as session:
            work_list = session.query(WorkList). \
                filter(WorkList.work_list_id == work_list_id).first()
            sub_delete_id = work_list.work_list_id
            session.delete(work_list)
        TicketSubResult.objects(work_list_id=sub_delete_id).delete()
        self.resp(msg="已删除")


class TicketExportHandler(TicketReq):

    async def get(self):
        """导出工单"""
        params = self.get_query_args(Schema({
            "work_list_id": scm_int
        }))
        work_list_id = params.pop("work_list_id")

        # 首先获取主工单的基本信息
        with make_session() as session:
            work_list = session.query(WorkList). \
                filter(WorkList.work_list_id == work_list_id).first()
            work_list = work_list.to_dict()
            work_list['work_list_status'] = \
                ALL_OFFLINE_TICKET_STATUS_CHINESE[work_list['work_list_status']]
            work_list['submit_date'] = str(work_list['submit_date']) \
                if work_list['submit_date'] else ''
            work_list['audit_date'] = str(work_list['audit_date']) \
                if work_list['audit_date'] else ''
            work_list['online_date'] = str(work_list['online_date']) \
                if work_list['online_date'] else ''

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
            work_sub_list = SubTicket.objects(ticket_id=work_list_id).all()
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

            await AsyncTimeout(10).async_thr(
                past.utils.utils.create_worklist_xlsx, filename, params_dict)
            self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})
