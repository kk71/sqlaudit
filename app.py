#!/usr/bin/env python3
# Author: kk.Fang(fkfkbill@gmail.com)

from utils.version_utils import get_versions
from utils.datetime_utils import *

__VERSION__ = ".".join([str(i) for i in get_versions()["versions"][-1]["version"]])

print(f"SQL-Audit version {__VERSION__} (process started at {dt_to_str(arrow.now())})")

from startup import *


if __name__ == "__main__":
    make_startup_check()
    collect_startup_scripts()
    cli()
