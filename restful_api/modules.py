# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["as_view"]

import importlib
from os import path
from glob import glob
from pathlib import Path

import settings
from .base import *


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


def as_view(route_rule: str = ""):
    """请求装饰器"""

    if route_rule:
        assert route_rule.strip()[0] != "/"

    def as_view_inner(req_handler: BaseReq):
        import restful_api.urls
        split_path = [
            i
            for i in req_handler.__module__.split(".")
            if i.lower() not in ("", "restful_api", "__init__")
        ]
        route = "/" / Path("/".join(split_path)) / route_rule
        restful_api.urls.urls.append(
            (str(route), req_handler)
        )
        return req_handler

    return as_view_inner
