# Author: kk.Fang(fkfkbill@gmail.com)

import importlib
from pathlib import Path
from glob import glob

import settings
from models import init_models

# initiate database models/connections

init_models()

from models import base, engine


def main():
    """create all tables for mysql"""

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
        if to_import[-3:].lower() != ".py":
            continue
        to_import = to_import[:-3]
        importlib.import_module(f"{'.'.join(filename)}.{to_import}")
    base.metadata.create_all(engine)
