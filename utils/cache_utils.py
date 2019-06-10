# Author: kk.Fang(fkfkbill@gmail.com)
"""
simple cache using redis, caches result of function,
 supporting manual/automatic expiration.
"""

import pickle
import json
import hashlib
from functools import wraps

import redis

import settings


def cache(
    redis_ip=settings.CACHE_REDIS_SERVER,
    redis_port=settings.CACHE_REDIS_PORT,
    redis_db=settings.CACHE_REDIS_DB,
    serializer=pickle,
    cache_top_domain: str = "REDIS-CACHE",
    cache_domain: str = None,
    default_cached: tuple = (),
    kwargs_excluded: tuple = ()
):
    """
    简单的缓存装饰器
    :param redis_ip:
    :param redis_port:
    :param redis_db:
    :param serializer: 序列化工具，默认用pickle，如果需要更快的序列化，可使用json（注意json的限制）
    :param cache_top_domain: 顶层缓存域，通常不应该修改
    :param cache_domain: 当前方法的缓存域，缺省为(模块名, 函数名)
    :param default_cached: 当缓存被清空之后，默认增加的缓存的参数，tuple of dicts
    :param kwargs_excluded: 不需要序列化匹配的入参，大部分实例对象。如果该参数成立，则入参必须都带参数名。
    :return:
    """

    def cache_dec(func):

        if not cache_domain:
            cache_domain = f"{func.__module__}-{func.__name__}"
        CACHE_KEY_TEMPLATE = f"{cache_top_domain}:{cache_domain}:" + "{args}:{kwargs}"

        def expire_top_domain():
            """清空当前顶级域的cache"""
            return

        def expire_domain():
            """清空当前函数域的cache"""
            return

        @wraps
        def decorated_func(*args, **kwargs):
            # do something check if there are cache
            ret = no_cache_func(*args, **kwargs)
            # if there aren't cache, save it.
            return ret

        @wraps
        def no_cache_func(*args, **kwargs):
            ret = func(*args, **kwargs)
            return ret

        decorated_func.expire_top_domain = expire_top_domain
        decorated_func.expire_domain = expire_domain
        decorated_func.no_cache_func = no_cache_func

        return decorated_func
    return cache_dec

