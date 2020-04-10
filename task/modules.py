# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "collect_dynamic_modules"
]

import importlib
from glob import glob
from pathlib import Path

import settings


def collect_dynamic_modules(module_names: [str]):
    """通过外层模块名，收集任务"""
    print("collecting dynamic module tasks ...")
    module_dirs = []
    for module_name in module_names:
        module_dirs += glob(
            str(Path(settings.SETTINGS_FILE_DIR) / f"{module_name}/**/tasks.py"),
            recursive=True
        )
        module_dirs += glob(
            str(Path(settings.SETTINGS_FILE_DIR) / f"{module_name}/**/tasks/**/*.py"),
            recursive=True
        )
    print(module_dirs)
    for module_dir in module_dirs:
        relative_path = module_dir[len(settings.SETTINGS_FILE_DIR):]
        # TODO only support *nix system
        py_file_dot_split = [i for i in relative_path.split("/") if i]
        py_file_path_for_importing = ".".join(py_file_dot_split)[:-3]
        importlib.import_module(f"{py_file_path_for_importing}")
