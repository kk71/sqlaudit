
from .base import AuthReq

from schema import Schema


class SelfHelpOnline(AuthReq):

    def get(self):
        params=self.get_query_args(Schema({

        }))
        self.resp()