# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "arrow",
    "date",
    "datetime",
    "COMMON_DATE_FORMAT",
    "COMMON_TIME_FORMAT",
    "COMMON_DATETIME_FORMAT",
    "dt_to_str",
    "d_to_str"
]

from typing import Union

import arrow
from datetime import date, datetime

from utils.const import COMMON_DATETIME_FORMAT, COMMON_DATE_FORMAT, COMMON_TIME_FORMAT


def dt_to_str(dt: Union[datetime, arrow.Arrow, list, tuple, dict]):
    if isinstance(dt, (datetime, arrow.Arrow)):
        return arrow.get(dt).format(fmt=COMMON_DATETIME_FORMAT)

    elif isinstance(dt, (list, tuple)):
        return [dt_to_str(i) for i in dt]

    elif isinstance(dt, dict):
        return {k: dt_to_str(v) for k, v in dt.items()}

    else:
        return dt


def d_to_str(d: Union[datetime, arrow.Arrow, date]) -> Union[str, None]:
    if not d:
        return None
    return arrow.get(d).format(fmt=COMMON_DATE_FORMAT)
