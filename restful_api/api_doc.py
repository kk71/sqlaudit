# Author: kk.Fang(fkfkbill@gmail.com)

from .base import *


class APIDocHandler(BaseReq):

    def get(self):
        from .modules import url_table
        s = url_table.get_html_string()
        self.finish(s)
