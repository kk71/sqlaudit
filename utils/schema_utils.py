# Author: kk.Fang(fkfkbill@gmail.com)

import arrow
from schema import And, Use,Or

from utils import const

__all__ = [
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
    "scm_datetime",
    "scm_bool",
    "scm_week_or_month_int",
]


# for string
scm_str = Use(str)
scm_unempty_str = And(scm_str, lambda x: len(x.strip()) > 0)
scm_str_with_no_lr_spaces = And(scm_str, Use(lambda x: x.strip()))
scm_something_split_str = lambda splitter, p=scm_str: \
    Use(lambda x: [p.validate(i.strip()) for i in x.split(splitter) if i.strip()])
scm_dot_split_str = scm_something_split_str(",", scm_unempty_str)
scm_subset_of_choices = lambda choices: lambda subset: set(subset).issubset(set(choices))
scm_one_of_choices = lambda choices: lambda x: x in choices

# for integer and float
scm_float = Use(float)
scm_int = Use(int)
scm_dot_split_int = scm_something_split_str(",", scm_int)

# for bool(real boolean or string transformed)
scm_bool = Use(lambda x: x not in (0, "0", False))

# for date and time
scm_datetime = Use(lambda x: arrow.get(x, const.COMMON_DATETIME_FORMAT).datetime)
scm_date = Use(lambda x: arrow.get(x, const.COMMON_DATE_FORMAT).date())

#week_int or month_int
scm_week_or_month_int=Or(lambda x: x==8)
