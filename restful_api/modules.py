# Author: kk.Fang(fkfkbill@gmail.com)

from .base import *


def as_view(route_rule: str):
    def as_view_inner(req_handler: BaseReq):
        import restful_api.urls
        restful_api.urls.dynamic_urls.append((route_rule, req_handler))
        return req_handler

    return as_view_inner
