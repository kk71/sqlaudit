# Author: kk.Fang(fkfkbill@gmail.com)

import arrow
from schema import And, Or, Use

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
    "scm_time",
    "scm_datetime",
    "scm_bool",
]

scm_any = Use(lambda x: x)
scm_str = Use(str)
scm_int = Use(int)
scm_float = Use(float)
scm_unempty_str = And(scm_str, lambda x: len(x.strip()) > 0)
scm_str_with_no_lr_spaces = And(scm_str, Use(lambda x: x.strip()))
scm_something_split_str = lambda splitter, p=scm_any: Use(lambda x: [p.validate(i.strip()) for i in x.split(splitter) if i.strip()])
scm_dot_split_str = scm_something_split_str(",", scm_unempty_str)
scm_dot_split_int = scm_something_split_str(",", scm_int)
scm_subset_of_choices = lambda choices: lambda subset: set(subset).issubset(set(choices))
scm_one_of_choices = lambda choices, p=scm_any: lambda x: p.validate(x) in choices
scm_date = Use(lambda x: arrow.get(x, ['MM/DD/YYYY', 'YYYY-MM-DD']).date())
scm_time = Use(lambda x: arrow.get(x, ['HH:mm:ss']).time())
scm_datetime = Use(lambda x: arrow.get(x, [
    'YYYY-MM-DD HH:mm:ss'
]).datetime)
scm_bool = Use(lambda x: x not in (0, "0", False))

