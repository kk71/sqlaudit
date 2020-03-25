# Author: kk.Fang(fkfkbill@gmail.com)

import utils.const
import ticket.const
import ticket.restful_api.online
from utils.schema_utils import *
from utils.datetime_utils import *
from ..ticket import OracleTicket


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


class OracleTicketOnlineExecuteHandler(
        ticket.restful_api.online.OnlineExecuteHandler):

    def post(self):
        pass
