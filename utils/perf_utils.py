# Author: kk.Fang(fkfkbill@gmail.com)

import time
from functools import wraps

__ALL__ = [
    "timing"
]


def timing(method):
    """函数计时"""
    @wraps(method)
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()

        print(f"""
  {method.__name__} in {method.__code__.co_filename}:{method.__code__.co_firstlineno}
    args: {args}, kwargs: {kwargs}
    {round(te - ts, 3)} seconds
""")
        return result
    return timed
