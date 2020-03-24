# Author: kk.Fang(fkfkbill@gmail.com)

from restful_api.views.base import *


@as_view("/huhu")
class AAA(BaseReq):

    def get(self):
        self.resp([])