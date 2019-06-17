from schema import Schema, Optional

from .base import AuthReq
from utils.schema_utils import *
from utils.const import *
from models.oracle import *


class OptimizeResultsHandler(AuthReq):

    def get(self):
        """智能优化结果"""
        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)

        with make_session() as session:
            results = session.query(AituneSqlExPlan)
            if keyword:
                results = self.query_keyword(results, keyword,
                                             AituneSqlExPlan.aituneid,
                                             AituneSqlExPlan.targetname,
                                             AituneSqlExPlan.sql_id)
            results, p = self.paginate(results, **p)
            results = [x.to_dict() for x in results]
            self.resp(results, **p)


class OptimizeDetailsHandler(AuthReq):

    def get(self):
        """智能优化详情"""
        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)

        with make_session() as session:
            details = session.query(AituneResultDetails)
            if keyword:
                details = self.query_keyword(details, keyword,
                                             AituneResultDetails.aituneid,
                                             AituneResultDetails.targetname,
                                             AituneResultDetails.sql_id)
            details, p = self.paginate(details, **p)
            details = [x.to_dict() for x in details]
            self.resp(details, **p)


class OptimizeBeforePlan(AuthReq):

    def get(self):
        """优化前SQL"""
        params = self.get_query_args(Schema({
            Optional("sql_id", default=None): scm_str,
            Optional("targetname", default=None): scm_str,

            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        sql_id, targetname = params.pop('sql_id'), params.pop('targetname')
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        del params

        if not (sql_id and targetname):
            return self.redirect("/api/cmdb/cmdbs")
        with make_session() as session:
            before_plan = session.query(AituneSqlExPlan). \
                filter(AituneSqlExPlan.sql_id == sql_id,
                       AituneSqlExPlan.targetname == targetname,
                       AituneSqlExPlan.flag == AI_TUNE_PRE_OPTIMIZED)
            if keyword:
                before_plan = self.query_keyword(before_plan, keyword,
                                                 AituneSqlExPlan.sql_id,
                                                 AituneSqlExPlan.aituneid,
                                                 AituneSqlExPlan.targetname)
            before_plan = self.paginate(before_plan, **p)
            before_plan = [x.to_dict() for x in before_plan]
            self.resp(before_plan, **p)


class OptimizeAfterPlan(AuthReq):

    def get(self):
        """优化后SQL"""
        params = self.get_query_args(Schema({
            Optional("sql_id", default=None): scm_str,

            Optional("targetname", default=None): scm_str,

            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        sql_id, targetname = params.pop('sql_id'), params.pop('targetname')
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        del params

        if not (sql_id and targetname):
            return self.redirect("/api/cmdb/cmdbs")
        with make_session() as session:
            after_plan = session.query(AituneSqlExPlan). \
                filter(AituneSqlExPlan.sql_id == sql_id,
                       AituneSqlExPlan.targetname == targetname,
                       AituneSqlExPlan.flag == AI_TUNE_POST_OPTIMIZED)
            if keyword:
                after_plan = self.query_keyword(after_plan, keyword,
                                                AituneSqlExPlan.sql_id,
                                                AituneSqlExPlan.targetname,
                                                AituneSqlExPlan.aituneid
                                                )
            after_plan = self.paginate(after_plan, **p)
            after_plan = [x.to_dict() for x in after_plan]
            self.resp(after_plan, **p)


class OptimizeHistoryPlan(AuthReq):

    def get(self):
        """历史执行SQL"""
        params = self.get_query_args(Schema({
            Optional("sql_id", default=None): scm_str,

            Optional("targetname", default=None): scm_str,

            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        sql_id, targetname = params.pop('sql_id'), params.pop('targetname')
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        del params

        if not (sql_id and targetname):
            return self.redirect("/api/cmdb/cmdbs")
            # TODO

        with make_session() as session:

            history_plan = session.query(AituneHistSqlStat). \
                filter(AituneHistSqlStat.sql_id == sql_id,
                       AituneHistSqlStat.targetname == targetname)
            if keyword:
                history_plan = self.query_keyword(history_plan, keyword,
                                                  AituneHistSqlStat.sql_id,
                                                  AituneHistSqlStat.targetname,
                                                  AituneHistSqlStat.snap_id)
            history_plan = self.paginate(history_plan, **p)
            history_plan = [x.to_dict() for x in history_plan]
            self.resp(history_plan, **p)
