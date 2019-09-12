# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "async_thr",
    "async_prc"
]

from asyncio import Future
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from tornado.concurrent import chain_future

import settings


thread_executor = ThreadPoolExecutor(settings.CONC_MAX_THREAD)
process_executor = ProcessPoolExecutor(settings.CONC_MAX_PROCESS)


def conc(func, executor, *args, **kwargs) -> Future:
    async_future = Future()
    print(f"* {func} submitted as async")
    conc_future = executor.submit(func, *args, **kwargs)
    chain_future(conc_future, async_future)
    return async_future


def async_thr(func, *args, **kwargs) -> Future:
    return conc(func, thread_executor, *args, **kwargs)


def async_prc(func, *args, **kwargs) -> Future:
    return conc(func, process_executor, *args, **kwargs)
