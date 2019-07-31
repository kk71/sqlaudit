import time
from schema import Schema, And
from sqlalchemy import func

from utils.datetime_utils import *
from .base import PrivilegeReq
from utils.schema_utils import *
from utils.const import *
from models.oracle import *
from utils import cmdb_utils

import past.utils.check


class OverviewHandler(PrivilegeReq):

    def get(self):
        self.acquire(PRIVILEGE.PRIVILEGE_SELF_SERVICE_ONLINE)

        """上线情况概览"""
        params = self.get_query_args(Schema({
            "duration": And(scm_unempty_str, scm_one_of_choices(("week", "month"))),
        }))
        duration = params.pop("duration")
        del params
        date_start = arrow.now().shift(**{f"{duration}s": -1}).datetime

        with make_session() as session:
            cmdb_ids: list = cmdb_utils.get_current_cmdb(session, self.current_user)
            if not cmdb_ids:
                return self.resp_forbidden(msg="当前登录用户的角色未拥有任何纳管数据库。")

            # 成功次数、失败次数, 上线次数(3+4)
            qe = QueryEntity(
                WorkList.work_list_status,
                func.count().label('work_list_status_count')
            )
            worklist_online = dict(session.query(*qe).
                filter(WorkList.work_list_status.in_([
                           OFFLINE_TICKET_FAILED, OFFLINE_TICKET_EXECUTED]),
                       WorkList.cmdb_id.in_(cmdb_ids),
                       WorkList.online_date > date_start).
                group_by(WorkList.work_list_status))
            worklist_online = {
                "ticket_succeed": worklist_online.get(OFFLINE_TICKET_EXECUTED, 0),
                "ticket_fail": worklist_online.get(OFFLINE_TICKET_FAILED, 0)
            }
            online_times = sum(worklist_online.values())

            # 在业务维度 展示上线次数
            qe = QueryEntity(
                WorkList.system_name,
                func.count().label('system_name_count')
            )
            business = session.query(*qe).\
                filter(WorkList.work_list_status == OFFLINE_TICKET_EXECUTED,
                       WorkList.cmdb_id.in_(cmdb_ids),
                       WorkList.online_date > date_start).group_by(WorkList.system_name)
            business = [qe.to_dict(x) for x in business]

            # 上线成功的语句数
            qe = QueryEntity(
                SubWorkList.status,
                func.count().label('sub_worklist_online_count')
            )
            sub_worklist_online = dict(session.query(*qe).
                join(WorkList, WorkList.work_list_id == SubWorkList.work_list_id).
                filter(
                SubWorkList.online_date > date_start,
                WorkList.cmdb_id.in_(cmdb_ids)).
                group_by(SubWorkList.status))
            sub_worklist_online = {
                "sql_succeed": sub_worklist_online.get(True, 0),
                "sql_fail": sub_worklist_online.get(False, 0),
            }

            # 展示待上线的脚本
            qe = QueryEntity(
                WorkList.task_name,
                WorkList.work_list_id,
                WorkList.submit_owner,
                WorkList.system_name
            )
            scripts_ready = session.query(*qe).filter(
                WorkList.work_list_status == OFFLINE_TICKET_PASSED,
                WorkList.cmdb_id.in_(cmdb_ids))
            scripts_ready = [qe.to_dict(x) for x in scripts_ready]

            return self.resp({
                **worklist_online,
                **sub_worklist_online,
                "online_times": online_times,
                "business": business,
                "scripts_ready": scripts_ready
            })


class ExecuteHandler(PrivilegeReq):

    def post(self):
        """执行上线"""
        self.acquire(PRIVILEGE.PRIVILEGE_SELF_SERVICE_ONLINE)

        params = self.get_json_args(Schema({
            "work_list_id": scm_int
        }))
        work_list_id = params.pop("work_list_id")
        with make_session() as session:
            ticket = session.query(WorkList).\
                filter(WorkList.work_list_id == work_list_id).first()
            sub_ticket_q = session.query(SubWorkList).\
                filter(SubWorkList.work_list_id == work_list_id)
            cmdb = session.query(CMDB).filter(CMDB.cmdb_id == ticket.cmdb_id).first()

            last_online_err = None
            for sub_ticket in sub_ticket_q:
                online_date = datetime.now()
                start = time.time()
                cmdb_dict = cmdb.to_dict(iter_if=lambda k, v: k in (
                    "ip_address", "port", "user_name", "password", "service_name"))
                cmdb_dict["host"] = cmdb_dict.pop("ip_address")
                cmdb_dict["username"] = cmdb_dict.pop("user_name")
                cmdb_dict["sid"] = cmdb_dict.pop("service_name")
                err_msg = past.utils.check.Check.sql_online(
                    sub_ticket.sql_text, cmdb_dict, ticket.schema_name)
                if not err_msg:
                    elapsed = int((time.time() - start) * 1000)
                    sub_ticket.online_date = online_date
                    sub_ticket.online_owner = self.current_user
                    sub_ticket.elapsed_seconds = elapsed
                    sub_ticket.status = True

                else:
                    last_online_err = err_msg
                    sub_ticket.online_date = online_date
                    sub_ticket.online_owner = self.current_user
                    sub_ticket.status = False
                    sub_ticket.error_msg = err_msg
                session.add(sub_ticket)

            if last_online_err:
                ticket.audit_comments = last_online_err
                ticket.work_list_stautus = OFFLINE_TICKET_FAILED
            else:
                ticket.audit_comments = "上线成功"
                ticket.work_list_stautus = OFFLINE_TICKET_EXECUTED
            ticket.online_date = datetime.now()
            session.add(ticket)
            self.resp_created({
                "msg": last_online_err,
                "online_status": ticket.work_list_stautus
            })
