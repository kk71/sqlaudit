# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "thread_conc_dec",
    "make_process_conc"
]

import functools
from typing import Callable
from asyncio import Future
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from tornado.concurrent import chain_future


thread_executor = ThreadPoolExecutor(4)
process_executor = ProcessPoolExecutor(4)


def thread_conc_dec() -> Callable:
    """装饰器，只能用于线程异步"""
    def _make_concurrent(func) -> Callable:

        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Future:
            async_future = Future()
            conc_future = thread_executor.submit(func, *args, **kwargs)
            chain_future(conc_future, async_future)
            return async_future

        return wrapper
    return _make_concurrent


def make_process_conc(func, executor=process_executor) -> Callable:
    """可产生线程异步和子进程异步的包装器"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Future:
        async_future = Future()
        conc_future = executor.submit(func, *args, **kwargs)
        chain_future(conc_future, async_future)
        return async_future

    return wrapper
