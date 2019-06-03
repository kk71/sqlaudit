# Author: kk.Fang(fkfkbill@gmail.com)

__ALL__ = [
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


COMMON_DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'
COMMON_DATE_FORMAT = 'YYYY-MM-DD'
COMMON_TIME_FORMAT = 'HH:mm:ss'


def dt_to_str(dt: Union[datetime, arrow.Arrow]) -> Union[str, None]:
    if not dt:
        return None
    return arrow.get(dt).format(fmt=COMMON_DATETIME_FORMAT)


def d_to_str(d: Union[datetime, arrow.Arrow, date]) -> Union[str, None]:
    if not d:
        return None
    return arrow.get(d).format(fmt=COMMON_DATE_FORMAT)
