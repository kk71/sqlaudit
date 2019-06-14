# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And, Or

from .base import AuthReq
from utils.schema_utils import *
from utils.datetime_utils import *
from models.mongo import *
from models.oracle import *


class DashboardHandler(AuthReq):

    def get(self):
        """仪表盘"""
        params = self.get_query_args(Schema({

        }))
        self.resp()
