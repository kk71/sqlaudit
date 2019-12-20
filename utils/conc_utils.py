# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "AsyncTimeout",
    "async_thr",
    "async_prc",
    "TimeoutError"
]

from asyncio import Future, wait_for
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, TimeoutError

from tornado.concurrent import chain_future

import settings


def async_prc_initializer():
    from models import init_models
    init_models()


thread_executor = ThreadPoolExecutor(
    settings.CONC_MAX_THREAD, initializer=async_prc_initializer)
process_executor = ProcessPoolExecutor(
    settings.CONC_MAX_PROCESS)


def conc(func, executor, *args, **kwargs) -> Future:
    async_future = Future()
    print(f"* {func} submitted as async")
    conc_future = executor.submit(func, *args, **kwargs)
    chain_future(conc_future, async_future)
    return async_future


class AsyncTimeout:

    def __init__(self, timeout=120):
        self.timeout = timeout

    def async_thr(self, func, *args, **kwargs):
        return wait_for(conc(func, thread_executor, *args, **kwargs), timeout=self.timeout)

    def async_prc(self, func, *args, **kwargs):
        return wait_for(conc(func, process_executor, *args, **kwargs), timeout=self.timeout)


def async_thr(func, *args, **kwargs) -> Future:
    return AsyncTimeout().async_thr(func, *args, **kwargs)


def async_prc(func, *args, **kwargs) -> Future:
    return AsyncTimeout().async_prc(func, *args, **kwargs)
