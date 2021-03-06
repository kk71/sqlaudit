# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "timing",
    "r_cache"
]

import time
import pickle
from functools import wraps
from typing import Union, Any
from types import FunctionType
from utils import const

import redis

import settings

dict_keys = type(dict().keys())


def func_info(method):
    return f"{method.__name__} in {method.__code__.co_filename}:" \
           f"{method.__code__.co_firstlineno}"


class RedisCache:
    """简易的redis缓存机"""

    def __init__(self,
                 host=settings.CACHE_REDIS_IP,
                 port=settings.CACHE_REDIS_PORT,
                 db=settings.CACHE_REDIS_DB,
                 prefix="cache",
                 key_serializer=pickle,
                 cache_serializer=pickle,
                 key_type_exclude=(),
                 ttl=settings.CACHE_DEFAULT_EXPIRE_TIME,
                 ):
        self.prefix = prefix
        self.key_serializer = key_serializer
        self.cache_serializer = cache_serializer
        self.key_type_exclude = key_type_exclude
        self.ttl = ttl
        self.redis_conn = redis.StrictRedis(
            host=host,
            port=port,
            db=db
        )
        self.bound_functions = []

    def expire(self, func: FunctionType = None, no_prefetch=False) -> dict:
        key = f"{self.prefix}-*"
        if func:
            key = self.get_func_key(func)

        keys = self.redis_conn.keys(key)
        deleted_num = 0
        if keys:
            deleted_num = self.redis_conn.delete(*keys)
        p = {}
        if not no_prefetch:
            p = self.prefetch()
        return {"deleted_keys": deleted_num, **p}

    def prefetch(self, func: FunctionType = None) -> dict:
        prefetch_num = 0
        for a_func in self.bound_functions:
            if func and a_func != func:
                continue
            method = getattr(a_func, "prefetch", None)
            if not method:
                print(f"* function {func_info(a_func)} has no prefetch method.")
                continue
            try:
                print(f"fetching {func_info(a_func)} ...")
                ret = method()
            except Exception as e:
                print(f"* failed when prefetch function {func_info(a_func)}."
                      f"prefetch: {str(e)}")
                continue
            print(f"* prefetch {func_info(a_func)} returned with {ret}")
            prefetch_num += 1
        return {"prefetch_num": prefetch_num, "bound_funcs": len(self.bound_functions)}

    def get_func_key(self, func: FunctionType):
        return f"{self.prefix}-{func.__name__}-*"

    def should_exclude(self, obj):
        if self.key_type_exclude and isinstance(obj, self.key_type_exclude):
            return True
        return False

    @classmethod
    def prepare_to_serialize(cls, obj):
        if isinstance(obj, dict_keys):
            return list(obj)
        return obj

    def get_key(self,
                func: FunctionType,
                func_call_args: tuple,
                func_call_kwargs: dict
                ) -> str:
        """获取一个key"""
        func_call_args = [self.prepare_to_serialize(i) for i in func_call_args
                          if not self.should_exclude(i)]
        func_call_kwargs = {k: self.prepare_to_serialize(v)
                            for k, v in func_call_kwargs.items()
                            if not self.should_exclude(v)}
        args_dumped = self.key_serializer.dumps(func_call_args)
        func_call_kwargs = dict(sorted(func_call_kwargs.items()))
        kwargs_dumped = self.key_serializer.dumps(func_call_kwargs)
        return f"{self.prefix}-{func.__name__}-{args_dumped}-{kwargs_dumped}"

    def get(self, key) -> Any:
        """获取值"""
        cached_result = self.redis_conn.get(key)
        if cached_result:
            return self.cache_serializer.loads(cached_result)

    def set(self, key, value, ttl=None):
        """设置值"""
        if not ttl:
            ttl = self.ttl
        dumped_result = self.cache_serializer.dumps(value)
        self.redis_conn.setex(name=key, value=dumped_result, time=ttl)


def timing(
        enabled: bool = settings.TIMING_ENABLED,
        threshold: float = settings.TIMING_THRESHOLD,
        cache: RedisCache = None,
        ttl: int = None
):
    """函数计时"""

    def timing_wrap(method):

        @wraps(method)
        def timed(*args, **kwargs):

            if cache:
                if args:
                    raise const.CannotUsePositionArgs
                the_key = cache.get_key(timed, args, kwargs)
                cached_result = cache.get(the_key)
                if cached_result:
                    print(f"* using cache result for {func_info(timed)} at {the_key}")
                    return cached_result

            tiks = []

            ts = time.time()

            def tik(msg: str = None):
                """在过程中掐一下时间"""
                tt = time.time()
                s = f"{round(tt - ts, 3)}"
                if msg:
                    s += " " + str(msg)
                if enabled:
                    tiks.append(s)
                else:
                    # if the timing is disabled, the tik also should be printed.
                    print(s)

            timed.tik = tik

            result = method(*args, **kwargs)
            te = time.time()
            t_rst = round(te - ts, 3)

            if cache:
                cache.set(the_key, result, ttl=ttl)

            if enabled and (t_rst >= threshold or cache):
                leading_spaces = "\n      * "
                tiks_formatted = leading_spaces + leading_spaces.join(tiks) if tiks else ""
                r = f"""
      {func_info(method)}
        args: {args}, kwargs: {kwargs}{tiks_formatted}
        {t_rst} seconds total
    """
                if cache:
                    r += f"    result cached at {the_key} \n"

                print(r)

            return result

        if cache:
            # 添加到cache
            cache.bound_functions.append(timed)

        return timed

    return timing_wrap


from sqlalchemy.orm.session import Session

r_cache = RedisCache(
    key_type_exclude=Session,
)

if __name__ == "__main__":
    @timing(cache=r_cache)
    def test(a=None):
        test.prefetch = lambda: test("qwe")

        print(a)

        import time
        time.sleep(2)
        test.tik("aaa")
        time.sleep(2)
        test.tik("bbb")
        time.sleep(1)
        return a


    test.prefetch = lambda: test("hahahahaha")

    # r_cache.expire()

    test(None)
    test(1)
    test("123")
    test([1, 2, 3])
    test({1, 2, 3})
    test({"a": 123})
