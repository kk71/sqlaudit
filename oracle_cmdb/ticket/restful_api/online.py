# Author: kk.Fang(fkfkbill@gmail.com)

import time
import traceback

from cx_Oracle import DatabaseError

import utils.const
import ticket.const
import ticket.restful_api.online
from models.sqlalchemy import make_session
from utils.schema_utils import *
from utils.datetime_utils import *
from restful_api.modules import *
# from utils import cmdb_utils
from ..ticket import OracleTicket
from ..sub_ticket import OracleSubTicket
from ..analyse import OracleSubTicketAnalyse


@as_view("overview", group="ticket")
class OracleTicketOnlineOverviewHandler(
        ticket.restful_api.online.OnlineOverviewHandler):

    def get(self):

        self.acquire(utils.const.PRIVILEGE.PRIVILEGE_SELF_SERVICE_ONLINE)

        params = self.get_query_args(Schema({
            "duration": And(scm_unempty_str, scm_one_of_choices(("week", "month"))),
        }))
        duration = params.pop("duration")
        del params
        date_start = arrow.now().shift(**{f"{duration}s": -1}).datetime

        with make_session() as session:
            cmdb_ids: list = cmdb_utils.get_current_cmdb(session, self.current_user)

            # 成功次数、失败次数, 上线次数(3+4)
            ticket_onlined_q = OracleTicket.objects(
                cmdb_id__in=cmdb_ids,
                online_date__gt=date_start
            )

            worklist_online = {
                "ticket_succeed": ticket_onlined_q.filter(
                    status=ticket.const.TICKET_EXECUTED).count(),
                "ticket_fail": ticket_onlined_q.filter(
                    status=ticket.const.TICKET_FAILED).count()
            }
            online_times = sum(worklist_online.values())

            # 上线成功的语句数
            sub_ticket_q = OracleSubTicket.objects(cmdb_id__in=cmdb_ids)
            sub_worklist_online = {
                "sql_succeed": sub_ticket_q.filter(online_status=True).count(),
                "sql_fail": sub_ticket_q.filter(online_status=False).count()
            }

            # 展示待上线的脚本
            tickets_ready = OracleTicket.objects(
                status=ticket.const.TICKET_PASSED,
                cmdb_id__in=cmdb_ids
            ).order_by("-create_time")
            scripts_ready = [
                ticket_ready.to_dict()
                for ticket_ready in tickets_ready[:10]
            ]

            return self.resp({
                **worklist_online,
                **sub_worklist_online,
                "online_times": online_times,
                "scripts_ready": scripts_ready
            })


@as_view("execute", group="ticket")
class OracleTicketOnlineExecuteHandler(
        ticket.restful_api.online.OnlineExecuteHandler):

    def post(self):

        self.acquire(utils.const.PRIVILEGE.PRIVILEGE_SELF_SERVICE_ONLINE)

        params = self.get_json_args(Schema({
            "ticket_id": scm_unempty_str
        }))
        ticket_id = params.pop("ticket_id")

        try:
            with make_session() as session:
                the_ticket = OracleTicket.objects(ticket_id=ticket_id).first()
                sub_ticket_q = OracleSubTicket.objects(ticket_id=the_ticket.ticket_id)
                cmdb = session.query(CMDB).filter(CMDB.cmdb_id == the_ticket.cmdb_id).first()
                if not cmdb.allow_online:
                    return self.resp_forbidden("当前库不允许自助上线")

                oracle_sub_ticket_analysis = OracleSubTicketAnalyse(
                    cmdb=cmdb, ticket=ticket)

                last_online_err = None
                for sub_ticket in sub_ticket_q:
                    online_date = datetime.now()
                    start = time.time()
                    u_p = ticket.to_dict(
                        iter_if=lambda k, v: k in ("online_username", "online_password"),
                        iter_by=lambda k, v: getattr(cmdb,
                                                     {"online_username": "user_name",
                                                      "online_password": "password"}[k])
                        if not v else v
                    )
                    u_p["username"] = u_p.pop("online_username")
                    u_p["password"] = u_p.pop("online_password")
                    err_msg = oracle_sub_ticket_analysis.sql_online(
                        sub_ticket.sql_text,
                        **u_p
                    )
                    sub_ticket.online_operator = self.current_user
                    sub_ticket.online_date = online_date
                    if not err_msg:
                        sub_ticket.elapsed_seconds = int((time.time() - start) * 1000)
                        sub_ticket.online_status = True

                    else:
                        last_online_err = err_msg
                        sub_ticket.error_msg = \
                            oracle_sub_ticket_analysis.update_error_message(
                                "自助上线", err_msg, old_msg=sub_ticket.error_msg)
                        sub_ticket.online_status = False

                if last_online_err:
                    ticket.audit_comments = last_online_err
                    ticket.work_list_status = ticket.const.TICKET_FAILED
                else:
                    ticket.audit_comments = "上线成功"
                    ticket.work_list_status = ticket.const.TICKET_EXECUTED
                ticket.online_date = datetime.now()
                session.add(ticket)
                session.commit()
                sub_ticket.save()
                self.resp_created({
                    "msg": last_online_err,
                    "online_status": the_ticket.status
                })

        except DatabaseError as e:
            error_info = str(e)
            print(f"failed when executing SQL script: {error_info}")
            self.resp_bad_req(msg=error_info)
        except Exception as e:
            error_info = traceback.format_exc()
            self.resp_bad_req(msg=error_info)
