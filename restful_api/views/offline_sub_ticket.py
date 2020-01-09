# Author: kk.Fang(fkfkbill@gmail.com)

import arrow
import xlsxwriter
from os import path
from mongoengine import Q
from schema import Schema, Optional

import settings
from utils.const import *
from utils.offline_utils import *
from utils.schema_utils import *
from utils.datetime_utils import dt_to_str
from models.oracle import *
from models.mongo.offline_oracle import *


class SubTicketHandler(TicketReq):

    def filter_sub_ticket(self, session):
        params = self.get_query_args(Schema({
            Optional("error_type", default=None): scm_one_of_choices({
                "static", "dynamic", "all_with_problems", "all"
            }),
            Optional("start_time", default=None): scm_datetime,
            Optional("end_time", default=None): scm_datetime,
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
            q = q.filer(work_list_id__in=work_list_id_list)
        if error_type == "static":
            q = q.filter(static__ne=[])
        elif error_type == "dynamic":
            q = q.filter(dynamic__ne=[])
        elif error_type == "all_with_problems":
            # 前端写着叫问题子工单，但是页面其实能把没问题的子工单也搜索出来，
            # 这里就加一个过滤有问题的子工单的参数吧。
            q = q.filter(Q(static__ne=[]) |
                         Q(dynamic__ne=[]))
        elif error_type is None or error_type == "all":
            pass  # reserved but should be useless
        else:
            assert 0
        q = self.privilege_filter_sub_ticket(q,session)
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
            "statement_id": scm_int,#TODO 前端要改哦

            Optional("sql_text"): scm_unempty_str,
            Optional("comments"): scm_str,
        }))
        statement_id = params.pop("statement_id")

        statement_id = TicketSubResult.objects(statement_id=statement_id).first()
        if not statement_id:
            return self.resp_bad_req(msg=f"找不到编号为{statement_id}的临时sql session")
        statement_id = statement_id.from_dict(params)
        TicketSubResult.objects(statement_id).insert()
        self.resp_created(statement_id.to_dict())


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
                ws.write(row_num, 4,  "\n".join(
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

        # TODO 这个接口

        params = self.get_query_args(Schema({
            "db_type": scm_one_of_choices(ALL_SUPPORTED_DB_TYPE),
            "statement_id": scm_unempty_str,
        }))
        self.resp()


class SubTicketRuleHandler(TicketReq):

    def patch(self):
        """修改子工单内的规则，修改后重新计算工单的分数"""
        params = self.get_json_args(Schema({

        }))
        self.resp_created()
