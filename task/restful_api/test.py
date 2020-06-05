# Author: kk.Fang(fkfkbill@gmail.com)

from restful_api import *
from ..tasks import TestTask


@as_view()
class TestHandler(BaseReq):

    async def get(self):
        """test for task"""
        ret = await TestTask.async_shoot()
        await self.resp({"ret": ret})

    get.argument = {
        "querystring": {}
    }
