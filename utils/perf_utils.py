# Author: kk.Fang(fkfkbill@gmail.com)

__ALL__ = [
    "timing"
]

import time
from functools import wraps

import settings


def timing(method):
    """函数计时"""
    @wraps(method)
    def timed(*args, **kwargs):
        ts = time.time()
        result = method(*args, **kwargs)
        te = time.time()

        if settings.ENABLE_TIMING:
            print(f"""
  {method.__name__} in {method.__code__.co_filename}:{method.__code__.co_firstlineno}
    args: {args}, kwargs: {kwargs}
    {round(te - ts, 3)} seconds
""")
        return result
    return timed
