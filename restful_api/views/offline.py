# Author: kk.Fang(fkfkbill@gmail.com)

import uuid
from os import path
from collections import OrderedDict

import xlrd
import xlsxwriter
from schema import Schema, Optional, And
from sqlalchemy import or_

import settings
from utils.schema_utils import *
from utils.datetime_utils import *
from utils.const import *
from utils import sql_utils, stream_utils
from .base import AuthReq, PrivilegeReq
from models.mongo import *
from models.oracle import *
from task.offline_ticket import offline_ticket
from task.mail_report import timing_send_work_list_status
from utils.conc_utils import *

import plain_db.oracleob
import past.utils.utils


class OfflineTicketCommonHandler(PrivilegeReq):

    def privilege_filter_ticket(self, q):
        """根据登录用户的权限过滤工单"""
        if self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_ADMIN):
            # 超级权限，可看所有的工单
            pass

        elif self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL):
            # 能看:自己提交的工单+指派给自己所在角色的工单+自己处理了的工单
            q = q.filter(
                or_(
                    WorkList.submit_owner == self.current_user,
                    WorkList.audit_role_id.in_(self.current_roles()),
                    WorkList.audit_owner == self.current_user
                )
            )

        else:
            # 只能看:自己提交的工单
            q = q.filter(WorkList.submit_owner == self.current_user)
        return q

    def privilege_filter_sub_ticket(self, q, session):
        """根据登录用户的权限过滤子工单"""
        if self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_ADMIN):
            # 超级权限，可看所有子工单
            pass

        elif self.has(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL):
            # 能看:自己提交的子工单+指派给自己所在角色的子工单+自己处理过了的工单
            sq = session.query(WorkList.work_list_id). \
                filter(or_(
                    WorkList.submit_owner == self.current_user,
                    WorkList.audit_role_id.in_(self.current_roles()),
                    WorkList.audit_owner == self.current_user
            ))
            q = q.filter(SubWorkList.work_list_id.in_(sq))

        else:
            # 只能看:自己提交的子工单
            sq = session.query(WorkList.work_list_id).\
                filter(WorkList.submit_owner == self.current_user)
            q = q.filter(SubWorkList.work_list_id.in_(sq))

        return q


class TicketHandler(OfflineTicketCommonHandler):

    def get(self):
        """线下审核工单列表"""

        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE)

        params = self.get_query_args(Schema({
            Optional("work_list_status", default=None):
                And(scm_int, scm_one_of_choices(ALL_OFFLINE_TICKET_STATUS)),
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        work_list_status = params.pop("work_list_status")
        p = self.pop_p(params)
        del params

        with make_session() as session:
            q = session.query(WorkList).order_by(WorkList.work_list_id.desc())
            if work_list_status is not None:  # take care of the value 0!
                q = q.filter_by(work_list_status=work_list_status)
            if keyword:
                q = self.query_keyword(q, keyword,
                                       WorkList.work_list_id,
                                       WorkList.work_list_type,
                                       WorkList.cmdb_id,
                                       WorkList.schema_name,
                                       WorkList.task_name,
                                       WorkList.system_name,
                                       WorkList.database_name,
                                       WorkList.submit_owner,
                                       WorkList.audit_owner,
                                       WorkList.audit_role_id,
                                       WorkList.audit_comments
                                       )
            q = self.privilege_filter_ticket(q)
            items, p = self.paginate(q, **p)
            ret = []
            for ticket in items:
                r = session.query(SubWorkList).\
                    filter(SubWorkList.work_list_id == ticket.work_list_id).\
                    with_entities(
                        SubWorkList.static_check_results,
                        SubWorkList.dynamic_check_results
                    ).all()
                static_rst, dynamic_rst = zip(*r) if r else ((), ())
                ret_item = {
                    **ticket.to_dict(),
                    "result_stats": {
                        "static_problem_num": len([i for i in static_rst if i]),
                        "dynamic_problem_num": len([i for i in dynamic_rst if i])
                    }
                }
                ret.append(ret_item)
            self.resp(ret, **p)

    def post(self):
        """创建DDL，DML工单"""
        params = self.get_json_args(Schema({
            "work_list_type": scm_one_of_choices(ALL_SQL_TYPE),
            "cmdb_id": scm_int,
            "schema_name": scm_unempty_str,
            "audit_role_id": scm_gt0_int,
            "task_name": scm_unempty_str,
            "session_id": scm_unempty_str,
            "online_username": scm_str,
            "online_password": scm_str
        }))
        params["submit_owner"] = self.current_user
        session_id = params.pop("session_id")
        with make_session() as session:
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == params["cmdb_id"]).first()
            params["system_name"] = cmdb.business_name
            params["database_name"] = cmdb.connect_name
            ticket = WorkList(**params)
            session.add(ticket)
            session.commit()
            session.refresh(ticket)
            wlats = session.query(WorkListAnalyseTemp).\
                filter(WorkListAnalyseTemp.session_id == session_id).all()
            sqls = [i.to_dict(iter_if=lambda k, v: k in ("sql_text", "comments")) for i in wlats]
            offline_ticket.delay(work_list_id=ticket.work_list_id, sqls=sqls)
        self.resp_created(msg="已安排分析，请稍后查询分析结果。")

    def patch(self):
        """更新工单的审阅状态"""
        self.acquire(PRIVILEGE.PRIVILEGE_OFFLINE_TICKET_APPROVAL)

        params = self.get_json_args(Schema({
            "work_list_id": scm_int,
            Optional("audit_comments"): scm_str,
            "work_list_status": And(scm_int, scm_one_of_choices(ALL_OFFLINE_TICKET_STATUS))
        }))
        params["audit_date"] = datetime.now()
        params["audit_owner"] = self.current_user
        work_list_id = params.pop("work_list_id")
        with make_session() as session:
            session.query(WorkList).filter(WorkList.work_list_id == work_list_id).update(params)
            work_list=session.query(WorkList).filter(WorkList.work_list_id==work_list_id).first()
            timing_send_work_list_status.delay(work_list)
            # timing_send_work_list_status(work_list)
        return self.resp_created(msg="更新成功")


class ExportTicketHandler(AuthReq):

    # TODO 时间关系，本接口几乎是从旧代码迁移过来的。
    def filter_data(self, data, filter_datetime=True):
        # 过滤拿出来的数据
        if data is None:
            return ""
        if isinstance(data, datetime) and filter_datetime:
            return data.strftime('%Y-%m-%d %X')
        return data

    # TODO 时间关系，本接口几乎是从旧代码迁移过来的。
    async def get(self):
        """导出工单以及其下全部子工单"""

        params = self.get_query_args(Schema({
            "work_list_id": scm_int
        }))
        worklist_id = params.pop("work_list_id")

        # 首先获取主工单的基本信息
        sql = f"SELECT * FROM T_WORK_LIST WHERE WORK_LIST_ID = {worklist_id}"
        works = plain_db.oracleob.OracleHelper.select_dict(sql, one=True)

        if works['work_list_status'] == 0:
            works['work_list_status'] = '待审核'
        elif works['work_list_status'] == 1:
            works['work_list_status'] = '审核通过'
        elif works['work_list_status'] == 2:
            works['work_list_status'] = '审核拒绝'
        elif works['work_list_status'] == 3:
            works['work_list_status'] = "已上线"
        elif works['work_list_status'] == 4:
            works['work_list_status'] = "上线失败"
        elif works['work_list_status'] == 5:
            works['work_list_status'] = "正在匹配规则中"
        else:
            works['work_list_status'] = "未知状态"

        works['work_list_type'] = 'DDL分析任务' if works['work_list_type'] else 'DML分析任务'

        works['submit_date'] = str(works['submit_date']) if works['submit_date'] else ''
        works['audit_date'] = str(works['audit_date']) if works['audit_date'] else ''
        works['online_date'] = str(works['online_date']) if works['online_date'] else ''

        # 主要信息
        works_heads = ["工单ID", "工单类型", "CMDBID", "用户名", "任务名称", "业务系统名称", "数据库名称", "SQL数量", "提交时间", "提交人", "审核时间", "工单状态", "审核人", "审核意见", "上线时间"]
        worklist_data = list(works.values())

        filename = '_'.join(['工单信息', works["task_name"], d_to_str(arrow.now())]) + '.xlsx'

        parame_dict = {
            'works_heads': works_heads,
            'works_data': worklist_data

        }

        # 根据工单获得一些统计信息
        sql = f"""SELECT work_list_id, statement_id, sql_text, static_check_results, dynamic_check_results,
                             check_time, check_owner, online_date, online_owner, elapsed_seconds,
                             status, error_msg, comments
                      FROM T_SUB_WORK_LIST
                      WHERE WORK_LIST_ID = {worklist_id}"""

        subworks = [[self.filter_data(x) for x in subwork]
                    for subwork in plain_db.oracleob.OracleHelper.select_with_lob(sql, one=False, index=2)]

        sql_count = len(subworks)
        fail_count = len([item[3] or item[4] for item in subworks if item[3] or item[4]])

        # 静态错误的工单
        static_fail_works = [item for item in subworks if item[3]]
        static_fail_count = len(static_fail_works)

        # 动态错误的工单
        dynamic_fail_works = [item for item in subworks if item[4]]
        dynamic_fail_count = len(dynamic_fail_works)

        fail_heads = ['总脚本数', '失败脚本数', '静态失败数', '动态失败数']
        fail_data = [sql_count, fail_count, static_fail_count, dynamic_fail_count]

        parame_dict.update(
            {
                'fail_heads': fail_heads,
                'fail_data': fail_data
            }
        )

        # 获得静态错误的子工单
        static_fail_heads = ['SQL_ID', 'SQL文本', '静态检测结果']
        static_fail_data = [[item[1], item[2], item[3]] for item in static_fail_works]

        parame_dict.update(
            {
                'static_fail_heads': static_fail_heads,
                'static_fail_data': static_fail_data
            }
        )

        # 获得动态错误的子工单
        dynamic_fail_heads = ['SQL_ID', 'SQL文本', '动态检测结果']
        dynamic_fail_data = [[item[1], item[2], item[4]] for item in dynamic_fail_works]

        parame_dict.update(
            {
                'dynamic_fail_heads': dynamic_fail_heads,
                'dynamic_fail_data': dynamic_fail_data
            }
        )

        # 获得所有子工单
        all_work_heads = ['SQL_ID', 'SQL文本', '静态检测结果', '动态检测结果']
        all_work_data = [[item[1], item[2], item[3], item[4]] for item in subworks]

        parame_dict.update(
            {
                'all_work_heads': all_work_heads,
                'all_work_data': all_work_data
            }
        )
        await AsyncTimeout(10).async_thr(
            past.utils.utils.create_worklist_xlsx, filename, parame_dict)
        self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})


class SQLUploadHandler(AuthReq):

    @staticmethod
    def get_unique_random_session_id():
        """生成随机唯一的session id"""
        session_id = uuid.uuid4().hex
        return session_id

    def post(self):
        """上传一个sql文件，或者一个excel的sql文件"""
        if not len(self.request.files) or not self.request.files.get("file"):
            self.resp_bad_req(msg="未选择文件。")
            return
        params = self.get_query_args(Schema({
            "ticket_type": And(scm_int, scm_one_of_choices(ALL_SQL_TYPE)),
            "if_filter": scm_bool
        }))
        file_object = self.request.files.get("file")[0]
        ticket_type = params.pop("ticket_type")
        if_filter = params.pop("if_filter")

        # 以下大部参考旧代码，旧代码是两个接口，这里合并了，统一返回结构。
        sql_keywords = {
            SQL_DDL: ['drop', 'create', 'alter'],
            SQL_DML: ['update', 'insert', 'delete', 'select']
        }
        if if_filter:
            sql_keyword = sql_keywords.get(ticket_type, [])
        else:
            sql_keyword = [x for keywords in sql_keywords.values() for x in keywords]
        filename = file_object['filename']

        if filename.split('.')[-1].lower() in ['sql']:
            # SQL script
            if file_object['body'].startswith(b"\xef\xbb\xbf"):
                body = file_object['body'][3:]
                encoding = 'utf-8'
            else:
                encoding = stream_utils.check_file_encoding(file_object['body'])
                body = file_object['body']
            try:
                body = body.decode(encoding)
            except UnicodeDecodeError:
                body = body.decode('utf-8')
            body = body.replace("\"", "'")
            formatted_sqls = sql_utils.parse_sql_file(body, sql_keyword)
            # 以下返回结构应该与创建工单输入的sqls一致，方便前端对接
            sqls = [
                {
                    "sql_text": x,
                    "comments": ""
                } for x in formatted_sqls if x
            ]

        elif filename.split('.')[-1].lower() in ['xls', "xlsx", "csv", "xlt"]:
            # excel doc or csv
            the_xls_filepath = path.join(settings.UPLOAD_DIR, filename)
            with open(the_xls_filepath, "wb") as z:
                z.write(file_object.body)
            excel = xlrd.open_workbook(the_xls_filepath)
            sheet = excel.sheet_by_index(0)
            if sheet.nrows <= 3:
                self.resp_bad_req(msg="空文件。")
                return
            system_name = sheet.row_values(0)[1]
            database_name = sheet.row_values(0)[1]
            sql = [[x for x in sheet.row_values(row)[:2]] for row in range(3, sheet.nrows)]
            # 以下返回结构应该与创建工单输入的sqls一致，方便前端对接
            sqls = [
                {
                    "sql_text": x[0],
                    "comments": x[1]
                } for x in sql
            ]

        else:
            self.resp_bad_req(msg="文件不是SQL脚本，Excel文档或者CSV文档的任何一种。")
            return
        with make_session() as session:
            session_id = self.get_unique_random_session_id()
            to_add = [
                WorkListAnalyseTemp(session_id=session_id, **i) for i in sqls
            ]
            session.add_all(to_add)
            self.resp_created({"session_id": session_id})

    def get(self):
        """获取上传的临时sql数据"""
        params = self.get_query_args(Schema({
            "session_id": scm_str,
            Optional("keyword", default=None): scm_str,
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

    def patch(self):
        """编辑上传的临时sql数据"""
        params = self.get_json_args(Schema({
            "id": scm_str,
            Optional("sql_text"): scm_unempty_str,
            Optional("comments"): scm_str,
            Optional("delete", default=False): scm_bool
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

    # def delete(self):
    #     """手动删除临时数据"""
    #     self.resp_created()


class SubTicketHandler(OfflineTicketCommonHandler):

    def filter_sub_ticket(self, session):
        params = self.get_query_args(Schema({
            Optional("error_type", default=None): scm_one_of_choices([
                "static", "dynamic", "all_with_problems", "all"
            ]),
            Optional("start_time", default=None): scm_datetime,
            Optional("end_time", default=None): scm_datetime,
            Optional("work_list_type"): scm_int,
            Optional("work_list_id"): scm_int,
            Optional("schema_name", default=None): scm_str,
            Optional("cmdb_id", default=None): scm_int,
            Optional("keyword", default=None): scm_str,

            Optional(object): object
        }))
        error_type = params.pop("error_type")
        keyword = params.pop("keyword")
        start_time, end_time = params.pop("start_time"), params.pop("end_time")
        schema_name = params.pop("schema_name")
        cmdb_id = params.pop("cmdb_id")

        to_filter = {k: v for k, v in params.items() if k in
                     ("work_list_type", "work_list_id")}

        # TODO 需要根据权限加过滤判断
        q = session.query(SubWorkList).filter_by(**to_filter)
        if cmdb_id:
            work_list_id_list = [i[0] for i in session.query(WorkList).
                filter_by(cmdb_id=cmdb_id).
                with_entities(WorkList.work_list_id).all()]
            q = q.filter(SubWorkList.work_list_id.in_(work_list_id_list))
        if start_time:
            q = q.filter(SubWorkList.check_time > start_time)
        if end_time:
            q = q.filter(SubWorkList.check_time < end_time)
        if keyword:
            q = self.query_keyword(q, keyword, SubWorkList.static_check_results,
                                   SubWorkList.dynamic_check_results,
                                   SubWorkList.comments)
        if schema_name:
            work_list_id_in_tuple = session.query(WorkList).\
                filter_by(schema_name=schema_name).\
                with_entities(WorkList.work_list_id).all()
            work_list_id_list = [i[0] for i in work_list_id_in_tuple]
            q = q.filter(SubWorkList.work_list_id.in_(work_list_id_list))
        if error_type == "static":
            q = q.filter(SubWorkList.static_check_results.isnot(None))
        elif error_type == "dynamic":
            q = q.filter(SubWorkList.dynamic_check_results.isnot(None))
        elif error_type == "all_with_problems":
            # 前端写着叫问题子工单，但是页面其实能把没问题的子工单也搜索出来，
            # 这里就加一个过滤有问题的子工单的参数吧。
            q = q.filter(
                SubWorkList.static_check_results.isnot(None) |
                SubWorkList.dynamic_check_results.isnot(None)
            )
        elif error_type is None or error_type == "all":
            pass  # reserved but should be useless
        else:
            assert 0
        q = self.privilege_filter_sub_ticket(q, session)
        return q

    def get(self):
        """子工单列表"""
        params = self.get_query_args(Schema({
            **self.gen_p(),
            Optional(object): object
        }))
        p = self.pop_p(params)
        with make_session() as session:
            q = self.filter_sub_ticket(session)
            items, p = self.paginate(q, **p)
            self.resp([i.to_dict() for i in items], **p)

    def patch(self):
        """修改子工单"""
        params = self.get_json_args(Schema({
            "id": scm_int,

            Optional("sql_text"): scm_unempty_str,
            Optional("comments"): scm_str,
        }))
        swl_id = params.pop("id")

        with make_session() as session:
            swl = session.query(SubWorkList).filter_by(id=swl_id).first()
            if not swl:
                return self.resp_bad_req(msg=f"找不到编号为{swl_id}的临时sql session")
            swl.from_dict(params)
            session.add(swl)
            session.commit()
            session.refresh(swl)
            self.resp_created(swl.to_dict())


class ExportSubTicketHandler(SubTicketHandler):

    def get(self):
        """
        花式导出子工单
        该接口与查询子工单共用同一套查询参数(见self.filter_sub_ticket)
        """
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
                q = session.query(SubWorkList).\
                    filter(SubWorkList.statement_id.in_(statement_id_list))
            else:
                assert 0

            # 写导出文档的地方参考旧代码

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
                      "检测时间", "检测人员", "上线时间", "上线人员", "执行时长(ms)",
                      "上线状态", "错误信息", "备注"]
            for x, field in enumerate(fields):
                ws.write(0, x, field.upper(), format_title)
            convert_datetime_to_str = lambda x: arrow.get(x).strftime(COMMON_DATETIME_FORMAT)
            for row_num, sub_ticket in enumerate(q.all()):
                row_num += 1
                ws.write(row_num, 0, sub_ticket.work_list_id, format_text)
                ws.write(row_num, 1, sub_ticket.statement_id, format_text)
                ws.write(row_num, 2, sub_ticket.sql_text, format_text)
                ws.write(row_num, 3, sub_ticket.static_check_results, format_text)
                ws.write(row_num, 4, sub_ticket.dynamic_check_results, format_text)
                ws.write(row_num, 5, convert_datetime_to_str(sub_ticket.check_time), format_text)
                ws.write(row_num, 6, sub_ticket.check_owner, format_text)
                ws.write(row_num, 7, convert_datetime_to_str(sub_ticket.online_date), format_text)
                ws.write(row_num, 8, sub_ticket.online_owner, format_text)
                ws.write(row_num, 9, sub_ticket.elapsed_seconds, format_text)
                ws.write(row_num, 10, sub_ticket.status, format_text)
                ws.write(row_num, 11, sub_ticket.error_msg, format_text)
                ws.write(row_num, 12, sub_ticket.comments, format_text)
            wb.close()
            self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})


class SubTicketSQLPlanHandler(AuthReq):

    # TODO 时间关系，本接口几乎是从旧代码迁移过来的。
    @staticmethod
    def get_right_width(column, column_width):
        times = 1
        count = column.count(' ')
        right_width = column_width - (len(column) - count) * times - count
        return " " * right_width + column + " "

    # TODO 时间关系，本接口几乎是从旧代码迁移过来的。
    @classmethod
    def get_plan_row(cls, plan_list):
        row_id_width = 4
        operation_width = 25
        name_width = 20
        rows_width = 7
        row_bytes_width = 9
        cost_width = 14
        time_width = 4

        row_id, operation, name, rows, row_bytes, cost, time = [str(x) for x in plan_list]

        row_id = cls.get_right_width(row_id, row_id_width)
        operation = cls.get_right_width(operation, operation_width)
        name = cls.get_right_width(name, name_width)
        rows = cls.get_right_width(rows, rows_width)
        row_bytes = cls.get_right_width(row_bytes, row_bytes_width)
        cost = cls.get_right_width(cost, cost_width)
        time = cls.get_right_width(time, time_width)

        content = '|' + '|\t'.join([row_id, operation, name, rows, row_bytes, cost, time]) + "|\n"
        return content, len(content) + 5

    def get(self):
        """获取子工单的sql执行计划"""
        params = self.get_query_args(Schema({
            "statement_id": scm_unempty_str
        }))
        with make_session() as session:
            sql_plan_row = session.query(OSQLPlan).filter_by(**params).first()
            if not sql_plan_row:
                return self.resp(msg="执行计划为空")
            hash_plan_value = sql_plan_row.plan_id

            sql_plans = MSQLPlan.objects(plan_hash_value=hash_plan_value,
                                         sql_id=params["statement_id"]).values_list("index", "operation",
                                                                                    "object_name", "cardinality",
                                                                                    "bytes", "cpu_cost", "time")
            sql_plan_head = OrderedDict({
                'Id': "ID",
                'Operation': "OPERATION",
                'Name': "OBJECT_NAME",
                'Rows': "CARDINALITY",
                'Bytes': "BYTES",
                'Cost (%CPU)': "CPU_COST",
                'Time': "TIME"
            })

            sql_plan_text_head, dash_len = self.get_plan_row(sql_plan_head.keys())
            # sql_plans = [[sql_plan[x] for x in sql_plan_head.values()] for sql_plan in sql_plans]
            sql_plan_text_content = ''.join([self.get_plan_row(row)[0] for row in sql_plans])

            dashes = "-" * dash_len
            sql_plan_text = f"""Plan hash value: {hash_plan_value}\n\n{dashes}\n{sql_plan_text_head}{dashes}\n{sql_plan_text_content}{dashes}\n"""
            self.resp({
                'sql_plan_text': sql_plan_text,
                # 'sql_plans': sql_plans
            })
