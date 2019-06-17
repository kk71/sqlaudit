# Author: kk.Fang(fkfkbill@gmail.com)

__ALL__ = [
    "timing"
]

import time
from functools import wraps

import settings


def timing(
        enabled: bool = settings.TIMING_ENABLED,
        threshold: float = settings.TIMING_THRESHOLD
):
    """函数计时"""

    def timing_wrap(method):

        @wraps(method)
        def timed(*args, **kwargs):

            tiks = []

            ts = time.time()

            def tik(msg: str = None):
                """在过程中掐一下时间"""
                tt = time.time()
                s = f"{round(tt - ts, 3)}"
                if msg:
                    s += " " + str(msg)
                tiks.append(s)

            timed.tik = tik

            result = method(*args, **kwargs)
            te = time.time()
            t_rst = round(te - ts, 3)

            if enabled and t_rst >= threshold:
                leading_spaces = "\n  * "
                tiks_formatted = leading_spaces + leading_spaces.join(tiks) if tiks else ""
                print(f"""
      {method.__name__} in {method.__code__.co_filename}:{method.__code__.co_firstlineno}
        args: {args}, kwargs: {kwargs}{tiks_formatted}
        {t_rst} seconds total
    """)
            return result
        return timed
    return timing_wrap


@timing()
def test():
    import time
    time.sleep(2)
    test.tik("aaa")
    time.sleep(2)
    test.tik("bbb")
    time.sleep(1)
    return


if __name__ == "__main__":
    test()
