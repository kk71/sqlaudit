import time
import traceback
from schema import Schema, And
from sqlalchemy import func
from cx_Oracle import DatabaseError

from utils.datetime_utils import *
from utils.offline_utils import *
from restful_api.views.base import PrivilegeReq
from utils.schema_utils import *
from utils.const import *
from models.oracle import *
from models.mongo import *
from models.mongo.offline import TicketSubResult
from utils import cmdb_utils
from .base import TicketReq


class OverviewHandler(PrivilegeReq):

    def get(self):
        """上线情况概览"""

        self.acquire(PRIVILEGE.PRIVILEGE_SELF_SERVICE_ONLINE)

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
            business = session.query(*qe). \
                filter(WorkList.work_list_status == OFFLINE_TICKET_EXECUTED,
                       WorkList.cmdb_id.in_(cmdb_ids),
                       WorkList.online_date > date_start).group_by(WorkList.system_name)
            business = [qe.to_dict(x) for x in business]

            # 上线成功的语句数
            work_list = session.query(WorkList).filter(WorkList.cmdb_id.in_(cmdb_ids))
            work_list = [x.to_dict()['work_list_id'] for x in work_list]

            to_collection = [{
                "$group": {'_id': "$online_status",
                           "count": {"$sum": 1}
                           }
            }]
            sub_worklist_online = TicketSubResult.objects(work_list_id__in=work_list,
                                                          online_date__gt=date_start). \
                aggregate(*to_collection)
            sub_worklist_online = list(sub_worklist_online)

            sub_worklist_online_count = {}
            for x in sub_worklist_online:
                sub_worklist_online_count[x['_id']] = x['count']
            sub_worklist_online = {
                "sql_succeed": sub_worklist_online_count.get(True, 0),
                "sql_fail": sub_worklist_online_count.get(False, 0),
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
                WorkList.cmdb_id.in_(cmdb_ids)
            ).order_by(WorkList.submit_date.desc())
            scripts_ready = [qe.to_dict(x) for x in scripts_ready]

            return self.resp({
                **worklist_online,
                **sub_worklist_online,
                "online_times": online_times,
                "business": business,
                "scripts_ready": scripts_ready
            })


class ExecuteHandler(TicketReq):

    def post(self):
        """执行上线"""
        self.acquire(PRIVILEGE.PRIVILEGE_SELF_SERVICE_ONLINE)

        db_type, params = self.alternative_args_db_type(
            self.get_json_args,
            oracle=Schema({
                "work_list_id": scm_int
            })
        )
        work_list_id = params.pop("work_list_id")

        try:
            with make_session() as session:
                ticket = session.query(WorkList). \
                    filter(WorkList.work_list_id == work_list_id).first()
                sub_ticket_q = TicketSubResult. \
                    objects(work_list_id=ticket.work_list_id)
                cmdb = session.query(CMDB).filter(CMDB.cmdb_id == ticket.cmdb_id).first()
                if not cmdb.allow_online:
                    return self.resp_forbidden("当前库不允许自助上线")

                if db_type == DB_ORACLE:

                    oracle_sub_ticket_analysis = OracleSubTicketAnalysis(
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
                            sub_ticket.error_msg = oracle_sub_ticket_analysis. \
                                update_error_message(
                                    "自助上线", err_msg, old_msg=sub_ticket.error_msg)
                            sub_ticket.online_status = False

                    if last_online_err:
                        ticket.audit_comments = last_online_err
                        ticket.work_list_status = OFFLINE_TICKET_FAILED
                    else:
                        ticket.audit_comments = "上线成功"
                        ticket.work_list_status = OFFLINE_TICKET_EXECUTED
                    ticket.online_date = datetime.now()
                    session.add(ticket)
                    session.commit()
                    sub_ticket.save()
                    self.resp_created({
                        "msg": last_online_err,
                        "online_status": ticket.work_list_status
                    })
                else:
                    assert 0

        except DatabaseError as e:
            error_info = str(e)
            print(f"failed when executing SQL script: {error_info}")
            self.resp_bad_req(msg=error_info)
        except Exception as e:
            error_info = traceback.format_exc()
            self.resp_bad_req(msg=error_info)
