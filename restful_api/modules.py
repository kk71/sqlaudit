# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "as_view"
]

import uuid
from typing import Optional, Dict, Tuple, Union

import importlib
from glob import glob
from pathlib import Path

import prettytable

import settings
from .base import *

# 用于展示api文档
url_table: Optional[prettytable.PrettyTable] = None


def collect_dynamic_modules(module_names: [str]):
    """通过外层模块名，收集动态url"""
    print("collecting dynamic module urls ...")
    module_dirs = []
    for module_name in module_names:
        module_dirs += glob(
            str(Path(settings.SETTINGS_FILE_DIR) / f"{module_name}/**/restful_api.py"),
            recursive=True
        )
        module_dirs += glob(
            str(Path(settings.SETTINGS_FILE_DIR) / f"{module_name}/**/restful_api/**.py"),
            recursive=True
        )
    for module_dir in module_dirs:
        relative_path = module_dir[len(settings.SETTINGS_FILE_DIR):]
        # TODO only support *nix system
        py_file_dot_split = [i for i in relative_path.split("/") if i]
        py_file_path_for_importing = ".".join(py_file_dot_split)[:-3]
        importlib.import_module(f"{py_file_path_for_importing}")


def pick_enabled_request_method(
        request_handler: BaseReq) -> Dict[str, Tuple[str, Dict[str, Union[dict, list]]]]:
    """抓取request handler里启用的请求方法，以及docstring"""
    ret = {}
    for request_method in request_handler.SUPPORTED_METHODS:
        the_method = getattr(request_handler, request_method.lower(), None)
        if not the_method.__doc__:
            continue
        ret[request_method] = the_method.__doc__, getattr(the_method, "argument", None)
    return ret


def as_view(route_rule: str = "", group: str = "", doc: bool = True):
    """
    请求装饰器
    :param route_rule: 相对的url路径，如果不传则以模块名作为路径
    :param group: 接口分组。方便归类
    :param doc: 是否在apidoc里展示
    :return:
    """

    if route_rule:
        assert route_rule.strip()[0] != "/"

    def as_view_inner(req_handler: BaseReq):
        import restful_api.urls
        split_path = [
            i
            for i in req_handler.__module__.split(".")
            if i.lower() not in ("", "restful_api", "__init__")
        ]
        route = "/api" / Path("/".join(split_path)) / route_rule
        restful_api.urls.urls.append(
            (str(route), req_handler)
        )
        if doc:
            for m, (docstring, argument) in pick_enabled_request_method(req_handler).items():
                restful_api.urls.verbose_structured_urls[group].append(
                    (str(route), m, docstring, argument, uuid.uuid4().hex, req_handler)
                )
        return req_handler

    return as_view_inner
