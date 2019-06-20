from .base import AuthReq
from utils.schema_utils import *
from utils import const

from models.oracle import *

from datetime import datetime
from datetime import timedelta
from schema import Schema, And
from sqlalchemy import func


class SelfHelpOnline(AuthReq):

    # @staticmethod
    # def online_success_rate(success,failed):
    #     online_success_rate=100 * success // ((failed + success) or 1)
    #     return {"online_success_rate":online_success_rate}

    def get(self):
        params = self.get_query_args(Schema({
            "duration": And(scm_int, scm_one_of_choices(const.WEEK_OR_MONTH)),
        }))
        duration = params.pop("duration")
        del params
        login_user = self.current_user

        with make_session() as session:
            if login_user != "admin":
                res = session.query(DataPrivilege).filter(DataPrivilege.login_user == login_user).with_entities(
                    DataPrivilege.cmdb_id)
            else:
                res = session.query(CMDB).with_entities(CMDB.cmdb_id)
            cmdb_ids = [x[0] for x in res]

            if cmdb_ids:
                time = datetime.now() - timedelta(days=duration)  # days=100测试
                # 上线次数、成功次数、失败次数。
                q = QueryEntity(func.count().label('work_list_status_count'), WorkList.work_list_status)
                worklist_online = session.query(*q). \
                    filter(WorkList.work_list_status.in_([const.REJECTED, const.HAS_BEEN_LAUNCHED]),
                           WorkList.cmdb_id.in_(cmdb_ids),
                           WorkList.online_date > time). \
                    group_by(WorkList.work_list_status)
                worklist_online = [list(x) for x in worklist_online]
                worklist_online = {{const.HAS_BEEN_LAUNCHED: "上线成功数量", const.REJECTED: "上线失败数量"}
                                   [x[1]]: x[0] for x in worklist_online}
                # worklist_online = [q.to_dict(x) for x in worklist_online]  # 前端接收work_list_status等于2为驳回，为3为上线

                online_times = sum(worklist_online.values())

                # 在业务维度 展示上线次数
                q = QueryEntity(WorkList.system_name, func.count().label('system_name_count'))
                business = session.query(*q).filter(WorkList.work_list_status == const.HAS_BEEN_LAUNCHED,
                                                    WorkList.cmdb_id.in_(cmdb_ids),
                                                    WorkList.online_date > time).group_by(WorkList.system_name)
                business = [q.to_dict(x) for x in business]

                # 饼图方式展现本月上线脚本数量
                q = QueryEntity(func.count().label('sub_worklist_online_count'), SubWorkList.status)
                sub_worklist_online = session.query(*q). \
                    join(WorkList, WorkList.work_list_id == SubWorkList.work_list_id). \
                    filter(
                    SubWorkList.online_date > time,
                    WorkList.cmdb_id.in_(cmdb_ids)). \
                    group_by(SubWorkList.status)
                sub_worklist_online = [list(x) for x in sub_worklist_online]
                sub_worklist_online = {{1: "脚本成功数量", 0: "脚本失败数量"}[x[1]]: x[0] for x in sub_worklist_online if
                                       x[1] != None}
                # sub_worklist_online = [q.to_dict(x) for x in sub_worklist_online]

                # 展示待上线的脚本
                q = QueryEntity(WorkList.task_name,
                                WorkList.work_list_id,
                                WorkList.submit_owner,
                                WorkList.system_name)
                scripts_ready = session.query(*q).filter(WorkList.work_list_status == const.THROUGH_AUDIT,
                                                         WorkList.cmdb_id.in_(cmdb_ids))
                scripts_ready = [q.to_dict(x) for x in scripts_ready]

                # failed=[x['work_list_status_count'] for x in worklist_online if x['work_list_status'] == const.REJECTED][0]
                # success=[x['work_list_status_count'] for x in worklist_online if x['work_list_status'] == const.HAS_BEEN_LAUNCHED][0]
                # online_success_rate=self.online_success_rate(success,failed)

                return self.resp({**worklist_online,
                                  **sub_worklist_online,
                                  "online_times": online_times,
                                  "business": business,
                                  "scripts_ready": scripts_ready,
                                  "duration": duration})
                # "online_success_rate":online_success_rate})
            else:
                return self.resp({
                    "上线失败数量": 0,
                    "上线成功数量": 0,
                    "脚本成功数量": 0,
                    "脚本失败数量": 0,
                    "online_times": 0,
                    "business": [],
                    "scripts_ready": [],
                    "duration": duration})
