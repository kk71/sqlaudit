# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = ["need_collect"]

import importlib
from typing import Union
from glob import glob
from pathlib import Path
from os import path

import settings
from .base import *

# 当前文件的所在目录
CURRENT_DIR = path.dirname(path.realpath(__file__))

CMDB_MODELS_TO_COLLECT = []

SCHEMA_OBJ_MODELS_TO_COLLECT = []

SQL_MODELS_TO_COLLECT = []


def collect_dynamic_modules():
    """通过外层模块名，收集动态采集的models"""
    print("collecting models to collect...")
    module_dirs = []
    module_dirs += glob(
        str(Path(CURRENT_DIR) / "**.py"),
        recursive=True
    )
    module_dirs += glob(
        str(Path(CURRENT_DIR) / "**/**.py"),
        recursive=True
    )
    for module_dir in module_dirs:
        relative_path = module_dir[len(settings.SETTINGS_FILE_DIR):]
        # TODO only support *nix system
        py_file_dot_split = [i for i in relative_path.split("/") if i]
        py_file_path_for_importing = ".".join(py_file_dot_split)[:-3]
        importlib.import_module(f"{py_file_path_for_importing}")


def need_collect(collecting_model: Union[SQLCapturingDoc,
                                         ObjectCapturingDoc,
                                         SchemaObjectCapturingDoc]):
    """需要采集的对象"""

    # object的两种采集model，判断的顺序很重要
    if isinstance(collecting_model, SchemaObjectCapturingDoc):
        SQL_MODELS_TO_COLLECT.append(collecting_model)
    elif isinstance(collecting_model, ObjectCapturingDoc):
        SQL_MODELS_TO_COLLECT.append(collecting_model)
    elif isinstance(collecting_model, SQLCapturingDoc):
        SQL_MODELS_TO_COLLECT.append(collecting_model)
    else:
        assert 0
    return collecting_model
