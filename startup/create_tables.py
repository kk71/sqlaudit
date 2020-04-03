# Author: kk.Fang(fkfkbill@gmail.com)

import importlib
from pathlib import Path
from glob import glob
from os import path

import settings
from models import init_models

# initiate database models/connections

init_models()

from models import base, engine


def main():
    """create all tables for mysql(没用)"""

    print("collecting sqlalchemy models ...")
    py_files = glob(str(Path(settings.SETTINGS_FILE_DIR) / "**/*.py"))
    for py_file in py_files:
        if "-" in py_file:
            continue
        filename = [
            i
            for i in py_file[len(settings.SETTINGS_FILE_DIR):].split("/")
            if i
        ]
        to_import = filename.pop()
        former, ext = path.splitext(to_import)
        if ext.lower() in (".pyc",):
            continue
        if former in ("__init__",):
            continue
        s = f"{'.'.join(filename)}.{former}"
        print(s)
        importlib.import_module(s)
    base.metadata.create_all(engine)
