# Author: kk.Fang(fkfkbill@gmail.com)

import uuid

import chardet
import settings
from os import path
from collections import defaultdict

from .base import *
from utils.offline_utils import *
from utils.const import *
from models.mongo.offline import *
from models.oracle import *
from utils import cmdb_utils
from utils.conc_utils import AsyncTimeout
from task.mail_report import timing_send_work_list_status
from task.offline_ticket import offline_ticket

import past.utils.utils


class TicketOuterHandler(TicketReq):

    def get(self):
        """线下工单的外层归档列表，按照日期，工单类型，审核结果来归档"""
        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE)

        params = self.get_query_args(Schema({
            "date_start": scm_date,
            "date_end": scm_date_end,
            **self.gen_p()
        }))
        p = self.pop_p(params)
        date_start = params.pop("date_start")
        date_end = params.pop("date_end")

        with make_session() as session:
            filtered_tickets = session.query(WorkList).filter(WorkList.submit_date >= date_start,
                                                              WorkList.submit_date < date_end). \
                order_by(WorkList.work_list_id.desc())
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
            scm_optional("work_list_status", default=None):
                And(scm_int, scm_one_of_choices(ALL_OFFLINE_TICKET_STATUS)),
            scm_optional("keyword", default=None): scm_str,
            scm_optional("date_start", default=None): scm_date,
            scm_optional("date_end", default=None): scm_date_end,
            scm_optional("work_list_id", default=None): scm_int,
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
                                       WorkList.audit_role_id,
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
                    }
                }
                ret.append(ret_item)

            self.resp(ret, **p)

    def post(self):
        """提交工单"""
        params = self.get_json_args(Schema({
            "cmdb_id": scm_int,
            "audit_role_id": scm_gt0_int,
            scm_optional("task_name", default=None): scm_unempty_str,
            "session_id": scm_unempty_str,
            scm_optional("online_username", default=None): scm_str,
            scm_optional("online_password", default=None): scm_str,

            # for oracle
            scm_optional("schema_name", default=None): scm_unempty_str,
        }))
        params["submit_owner"] = self.current_user
        session_id = params.pop("session_id")

        with make_session() as session:
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == params["cmdb_id"]).first()

            if cmdb.database_type in (DB_ORACLE, 1):

                ticket = WorkList(db_type=DB_ORACLE)
                sub_ticket_analysis = OracleSubTicketAnalysis(
                    cmdb=cmdb, ticket=ticket)
                if not cmdb_utils.check_cmdb_privilege(cmdb):
                    return self.resp_forbidden(
                        msg=f"当前纳管库的登录用户({cmdb.user_name})权限不足，"
                            "无法做诊断分析。"
                    )
                if not params["schema_name"]:
                    # 缺省就用纳管库登录的用户去执行动态审核（也就是explain plan for）
                    # 缺省的情况下，假设用户会在自己上传的sql语句里带上表的schema
                    # 如果他的sql不带上schema，则它必须在提交工单的时候指定sql运行的schema_name
                    # 否则无法确定他的对象是处在哪个schema下面的
                    # 默认的纳管库用户是需要打开权限的，以保证能够在访问别的schema的对象
                    # 所以需要在前面先验证纳管库登录的用户是否有足够的权限。
                    params["schema_name"] = cmdb.user_name
                params["system_name"] = cmdb.business_name
                params["database_name"] = cmdb.connect_name
                if not params["task_name"]:
                    params['task_name'] = sub_ticket_analysis.get_available_task_name(
                        submit_owner=params["submit_owner"]
                    )
                ticket.from_dict(params)
                session.add(ticket)
                session.commit()
                session.refresh(ticket)
                offline_ticket.delay(
                    work_list_id=ticket.work_list_id, session_id=session_id)

            elif cmdb.database_type == DB_MYSQL:

                pass

            self.resp_created(msg="已安排分析，请稍后查询分析结果。")

    def patch(self):
        """编辑工单状态"""
        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL)

        params = self.get_json_args(Schema({
            "work_list_id": scm_int,
            scm_optional("audit_comments"): scm_str,
            "work_list_status": And(scm_int,
                                    scm_one_of_choices(ALL_OFFLINE_TICKET_STATUS))
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
        params = self.get_query_args(Schema({
            "work_list_id": scm_int
        }))
        work_list_id = params.pop("work_list_id")
        del params

        with make_session() as session:
            work_list = session.query(WorkList). \
                filter(WorkList.work_list_id == work_list_id).first()
            sub_delete_id = work_list.work_list_id
            session.delete(work_list)
        self.resp(msg="已删除")
        work_sub_list = TicketSubResult.objects(work_list_id=sub_delete_id).all()
        work_sub_list.delete()


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
            work_sub_list = TicketSubResult. \
                objects(work_list_id=work_list_id).all()
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


class SQLUploadHandler(TicketReq):

    def get(self):
        """获取上传的临时sql数据"""
        params = self.get_query_args(Schema({
            "session_id": scm_str,
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        with make_session() as session:
            q = session.query(WorkListAnalyseTemp).filter_by(**params)
            if keyword:
                q = self.query_keyword(q, keyword,
                                       WorkListAnalyseTemp.sql_text,
                                       WorkListAnalyseTemp.comments)
            sqls, p = self.paginate(q, **p)
            self.resp([sql.to_dict() for sql in sqls], **p)

    def post(self):
        """上传一个sql脚本"""
        # TODO 目前没有对oracle和mysql的上传作区分，未来可能要考虑这点

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
        except UnicodeDecodeError:
            body = body.decode('utf-8')
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

    def patch(self):
        """编辑上传的临时sql数据"""
        params = self.get_json_args(Schema({
            "id": scm_str,
            scm_optional("sql_text"): scm_unempty_str,
            scm_optional("comments"): scm_str,
            scm_optional("sql_type"): scm_one_of_choices(ALL_SQL_TYPE),
            scm_optional("delete", default=False): scm_bool
        }))
        wlat_id = params.pop("id")
        delete = params.pop("delete")
        with make_session() as session:
            wlat = session.query(WorkListAnalyseTemp).filter_by(id=wlat_id).first()
            if not wlat:
                self.resp_bad_req(msg=f"找不到编号为{wlat_id}的临时sql session")
            if delete:
                session.delete(wlat)
                session.commit()
                self.resp_created(msg="sql已删除。")
            else:
                wlat.from_dict(params)
                session.add(wlat)
                session.commit()
                session.refresh(wlat)
                self.resp_created(wlat.to_dict())
