# Author: kk.Fang(fkfkbill@gmail.com)

from os import path

from mongoengine import Q

import settings
from restful_api.modules import *
from utils.datetime_utils import *
from utils.schema_utils import *
from .base import *
from .. import const
from ..sub_ticket import SubTicket
from ..ticket import Ticket
from ..tasks.sub_ticket_export import SubTicketExport


@as_view(group="ticket")
class SubTicketHandler(TicketReq):

    def filter_sub_ticket(self):
        params = self.get_query_args(Schema({
            scm_optional("ticket_id"): scm_object_id,
            scm_optional("cmdb_id"): scm_int,
            scm_optional("script_id", default=None): scm_unempty_str,
            scm_optional("schema_name", default=None): scm_str,
            scm_optional("error_type", default=None): scm_one_of_choices(
                const.ALL_SUB_TICKET_FILTERS),
            scm_optional("keyword", default=None): scm_str,
            scm_optional("order_by", default="position"): scm_unempty_str,
            **self.gen_date(),
        }, ignore_extra_keys=True))
        error_type = params.pop("error_type")
        keyword = params.pop("keyword")
        start_time, end_time = self.pop_date(params)
        schema_name = params.pop("schema_name")
        script_id = params.pop("script_id")
        order_by = params.pop("order_by")

        to_filter = {k: v for k, v in params.items() if k in
                     ("ticket_id", "cmdb_id")}
        del params

        # TODO 需要根据权限加过滤判断
        q = SubTicket.filter(**to_filter)
        if script_id:
            q = q.filter(script__script_id=script_id)
        if start_time:
            q = q.filter(create_time__gt=start_time)
        if end_time:
            q = q.filter(create_time__lt=end_time)
        if keyword:
            q = self.query_keyword(q, keyword,
                                   "static",
                                   "dynamic",
                                   "comments",
                                   "task_name"
                                   "sql_text")
        if schema_name:
            ticket_id_list: list = list(Ticket.filter(
                schema_name=schema_name).values_list("ticket_id"))
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
        q = self.privilege_filter_sub_ticket(q).order_by(order_by)
        return q

    def get(self):
        """子工单列表"""
        params = self.get_query_args(Schema({
            **self.gen_p(),
        }, ignore_extra_keys=True))
        p = self.pop_p(params)
        q = self.filter_sub_ticket()
        items, p = self.paginate(q, **p)
        self.resp([i.to_dict() for i in items], **p)

    get.argument = {
        "querystring": {
            "//ticket_id": 1,
            "//cmdb_id": 2526,
            "//script_id": "",
            "//schema_name": "",
            "//error_type": "static",
            "//keyword": "",
            "//order_by": "position",
            "//page": 1,
            "//per_page": 10
        }
    }

    def patch(self):
        """编辑子工单"""
        params = self.get_json_args(Schema({
            "statement_id": scm_unempty_str,

            scm_optional("sql_text"): scm_unempty_str,
            scm_optional("comments"): scm_str,
        }))
        statement_id = params.pop("statement_id")

        sub_ticket = SubTicket.filter(statement_id=statement_id).first()
        if not sub_ticket:
            return self.resp_bad_req(msg=f"找不到子工单编号为{statement_id}")
        sub_ticket.from_dict(params)
        sub_ticket.save()
        self.resp_created(sub_ticket.to_dict())

    patch.argument = {
        "json": {
            "statement_id": "juINDpGfSN2t3ukow7SoSw==",
            "//sql_text": "",
            "//comments": "xx"
        }
    }

    def delete(self):
        """删除子工单"""
        params = self.get_json_args(Schema({
            "statement_id": scm_unempty_str
        }))
        statement_id = params.pop("statement_id")

        the_sub_ticket = SubTicket.filter(statement_id=statement_id).first()
        if not the_sub_ticket:
            return self.resp_bad_req(msg="子工单未找到")
        the_script_id_to_this_sub_ticket = the_sub_ticket.script.script_id
        the_ticket = Ticket.filter(ticket_id=the_sub_ticket.ticket_id).first()
        if not the_ticket:
            return self.resp_bad_req(msg="工单未找到")
        if not the_sub_ticket:
            return self.resp_bad_req(msg=f"找不到子工单编号为{statement_id}")
        if SubTicket.filter(ticket_id=the_sub_ticket.ticket_id).count() == 1:
            return self.resp_bad_req(msg="删除失败，工单应该有至少一条语句。")
        the_sub_ticket.delete()
        for the_script in the_ticket.scripts:
            if the_script.script_id == the_script_id_to_this_sub_ticket:
                the_script.update_sub_ticket_count_from_sub_ticket()
        the_ticket.update_sub_ticket_count_from_scripts()
        the_ticket.save()
        self.resp_created(msg="删除成功。")

    delete.argument = {
        "json": {
            "statement_id": ""
        }
    }


@as_view("export", group="ticket")
class SubTicketExportHandler(SubTicketHandler):

    async def get(self):
        """导出子工单"""
        params = self.get_query_args(Schema({
            "export_type": scm_one_of_choices(["all", "selected"]),

            scm_optional(object): object
        }))
        export_type = params.pop("export_type")
        if export_type == "all":
            # 导出全部子工单按照条件过滤出来的结果
            q = self.filter_sub_ticket()
        elif export_type == "selected":
            # 给出子工单id，仅导出这些。
            params = self.get_query_args(Schema({
                "statement_id_list": scm_dot_split_str,
                scm_optional(object): object
            }))
            statement_id_list = params.pop("statement_id_list")
            q = SubTicket.filter(statement_id__in=statement_id_list)
        else:
            assert 0

        filename = f"export_sub_ticket_{dt_to_str(arrow.now())}.xlsx"

        await SubTicketExport.async_shoot(filename=filename, parame_dict=q)
        await self.resp({"url": path.join(settings.EXPORT_PREFIX, filename)})

    get.argument = {
        "querystring": {
            "export_type": "all",
            "//ticket_id": "5edef7e22a0c111df052f3b7",
            "//cmdb_id": "5",
            "//script_id": "7e1203a192194981bd75c27a3fb5dcf1",
            "//schema_name": "isqlaudit_dev",
            "//error_type": "static",

            "//statement_id_list": "LHEQDsDzTP2JakOelqw5cA==,6mTzrQzdTxCqnCVzNTy2hw==",
        }
    }
