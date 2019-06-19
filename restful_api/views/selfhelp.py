
from .base import AuthReq
from utils.schema_utils import *
from utils import const

from models.oracle import *

from datetime import datetime
from datetime import timedelta
from schema import Schema,And
from sqlalchemy import func



class SelfHelpOnline(AuthReq):

    def get(self):
        params=self.get_query_args(Schema({
            "duration":And(scm_int, scm_one_of_choices(const.WEEK_OR_MONTH)),
        }))
        duration=params.pop("duration")
        del params
        login_user=self.current_user

        with make_session() as session:
            if login_user != "admin":
                res=session.query(DataPrivilege).filter(DataPrivilege.login_user==login_user).with_entities(DataPrivilege.cmdb_id)
            else:
                res=session.query(CMDB).with_entities(CMDB.cmdb_id)#TODO with_ebtities
            cmdb_ids=[x[0] for x in res]


            if cmdb_ids:
                time=datetime.now()-timedelta(days=duration)
                # 上线成功次数、失败次数。 成功率
                worklist_online=session.query(func.count(),WorkList.work_list_status).\
                    filter(WorkList.work_list_status.in_([const.REJECTED,const.HAS_BEEN_LAUNCHED]),
                           WorkList.cmdb_id.in_(cmdb_ids),
                           ).\
                    group_by(WorkList.work_list_status)
                print(1)

            #
            #     worklist_online=1
            #     worklist_online={{3:"成功数量",4:"失败数量"}[x[1]]:x[0] for x in worklist_online}
            #
            #     # 在业务维度 展示上线次数
            #     session.query(WorkList.system_name,func.count()).filter(WorkList.work_list_status==const.REJECTED,
            #                                    WorkList.cmdb_id.in_(cmdb_ids),
            #                                    WorkList.online_date>time).group_by(WorkList.system_name)
            #     business=1
            #     business={x[0]:x[1] for x in business}
            #
            #     # 饼图方式展现本月上线脚本数量
            #     session.query(WorkList,func.count()).join(SubWorkList,WorkList.work_list_id==SubWorkList.work_list_id).\
            #         filter(SubWorkList.online_date>time,WorkList.cmdb_id.in_(cmdb_ids)).group_by(SubWorkList.status)
            #     sub_worklist_online=1
            #     sub_worklist_online={{1: "成功数量", 0: "失败数量"}[x[1]]:x[0]for x in sub_worklist_online}
            #
            #     # 展示待上线的脚本
            #     scripts_ready=session.query(WorkList).filter(WorkList.work_list_status==const.STAY_AUDIT,
            #                                    WorkList.cmdb_id.in_(cmdb_ids))
            #
            #     scripts_ready = [{'name': x[0] + " | " + x[3] + " | " + x[2], 'worklist_id': x[1]} for x in scripts_ready]
            #
            #     failed = worklist_online.get('失败数量', 0)
            #     success = worklist_online.get('成功数量', 0)
            #     return  self.resp({"online_times":sum(worklist_online.values()),
            #                        "scripts_ready":scripts_ready,
            #                        "worklist_online":worklist_online,
            #                        "sub_worklist_online":sub_worklist_online,
            #                        "business":business,
            #                        "online_count":100*success//((failed+success)or 1),
            #                        "duration":duration})
            # else:
            #     return self.resp({"online_times": 0,
            #                       "scripts_ready": [],
            #                       "worklist_online": {'成功数量': 0, '失败数量': 0},
            #                       "sub_worklist_online": {'成功数量': 0, '失败数量': 0},
            #                       "business": {'bussiness1': 0, 'bussiness2': 0},
            #                       "online_count": 0,
            #                       "duration": duration})