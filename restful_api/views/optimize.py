
from .base import AuthReq
from utils.schema_utils import *
from models.oracle import *

from schema import Schema,Optional


class OptimizeList(AuthReq):

    def get(self):
        """获得智能优化结果和优化详情"""
        params=self.get_query_args(Schema({
            Optional("keyword",default=None): scm_str,
            Optional("page",default=1): scm_int,
            Optional("per_page",default=10): scm_int,
        }))



        from models.oracle.optimize import *
        with make_session() as session:
            intelligent=session.query(AituneResultSummary)

            optimization_result=session.query(AituneResultDetails)


            self.resp({'data':1,'opt_data':2})


class OptimizeDetails(AuthReq):
    pass