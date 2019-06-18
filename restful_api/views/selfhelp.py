
from .base import AuthReq
from utils.schema_utils import *

from schema import Schema



class SelfHelpOnline(AuthReq):

    def get(self):
        params=self.get_query_args(Schema({
            "duration":scm_week_or_month_int
        }))
        duration=params.pop("duration")
        print(duration)
        self.resp({"duration":duration})