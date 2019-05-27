# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union
from datetime import datetime, date, time

import arrow
from schema import And, Or, Use

__all__ = [
    "COMMON_DATETIME_FORMAT",
    "scm_str",
    "scm_int",
    "scm_float",
    "scm_unempty_str",
    "scm_str_with_no_lr_spaces",
    "scm_something_split_str",
    "scm_dot_split_str",
    "scm_dot_split_int",
    "scm_subset_of_choices",
    "scm_one_of_choices",
    "scm_date",
    "scm_time",
    "scm_datetime",
    "scm_bool",
    "dt_to_str",
]

COMMON_DATETIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'
COMMON_DATE_FORMAT = 'YYYY-MM-DD'
COMMON_TIME_FORMAT = 'HH:mm:ss'

# a placeholder
scm_any = Use(lambda x: x)

# for integer and float
scm_float = Use(float)
scm_int = Use(int)
scm_something_split_str = lambda splitter, p=scm_any: \
    Use(lambda x: [p.validate(i.strip()) for i in x.split(splitter) if i.strip()])
scm_dot_split_int = scm_something_split_str(",", scm_int)

# for bool(real boolean or string transformed)
scm_bool = Use(lambda x: x not in (0, "0", False))

# for string
scm_str = Use(str)
scm_unempty_str = And(scm_str, lambda x: len(x.strip()) > 0)
scm_str_with_no_lr_spaces = And(scm_str, Use(lambda x: x.strip()))
scm_dot_split_str = scm_something_split_str(",", scm_unempty_str)
scm_subset_of_choices = lambda choices: lambda subset: set(subset).issubset(set(choices))
scm_one_of_choices = lambda choices, p=scm_any: lambda x: p.validate(x) in choices

# for date and time
scm_datetime = Use(lambda x: arrow.get(x, COMMON_DATETIME_FORMAT).datetime)
scm_date = Use(lambda x: arrow.get(x, COMMON_DATE_FORMAT).date())
scm_time = Use(lambda x: arrow.get(x, COMMON_TIME_FORMAT).time())


def dt_to_str(dt: Union[datetime, arrow.Arrow]) -> Union[str, None]:
    if not dt:
        return None
    return arrow.get(dt).format(fmt=COMMON_DATETIME_FORMAT)
