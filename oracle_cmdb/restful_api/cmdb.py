# Author: kk.Fang(fkfkbill@gmail.com)

from collections import defaultdict

from ..cmdb import *
from ..auth.user_utils import *
from utils.schema_utils import *
from utils.datetime_utils import *
from models.sqlalchemy import *
from restful_api.modules import as_view
from auth.restful_api.base import AuthReq
from oracle_cmdb.tasks.capture.cmdb_task_capture import oracle_latest_cmdb_score


@as_view("score_trend", group="cmdb")
class CMDBHealthTrendHandler(AuthReq):

    def post(self):
        """库健康评分趋势图"""
        params = self.get_json_args(Schema({
            scm_optional("cmdb_id_list", default=()): list
        }), default_body="{}")
        now = arrow.now()
        cmdb_id_list = params.pop("cmdb_id_list")

        with make_session() as session:
            if not cmdb_id_list:
                cmdb_id_list = current_cmdb(session, user_login=self.current_user)
                # 如果没有给出cmdb_id，则把最差的前十个拿出来
                cmdb_id_list = [
                                   i
                                   for i in oracle_latest_cmdb_score(session).keys()
                                   if i in cmdb_id_list
                               ][:10]
            fields = set()
            ret = defaultdict(dict)  # {date: [{health data}, ...]}
            for cmdb_id in cmdb_id_list:  # TODO stats
                dh_q = StatsCMDBRate.objects(
                    cmdb_id=cmdb_id,
                    etl_date__gt=now.shift(weeks=-2).datetime
                ).order_by("etl_date")
                for dh in dh_q:
                    ret[dh.etl_date.date()][dh.connect_name] = dh.score
                    fields.add(dh.connect_name)
            base_lines = [
                i[0]
                for i in session.
                    query(OracleCMDB.baseline).
                    filter(OracleCMDB.cmdb_id.in_(cmdb_id_list)).
                    order_by(OracleCMDB.baseline)
            ]
            if not base_lines or base_lines[0] == 0:
                base_line = 80
            else:
                base_line = base_lines[0]
            ret = [{
                "date": d_to_str(k),
                **v
            } for k, v in ret.items()]
            self.resp({
                "data": ret,
                "fields": list(fields),
                "base_line": base_line
            })
