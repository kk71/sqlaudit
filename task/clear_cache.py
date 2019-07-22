# Author: kk.Fang(fkfkbill@gmail.com)

from task.base import *
from models import init_models


@celery.task
def clear_cache(no_prefetch=False):

    init_models()

    from utils.perf_utils import r_cache
    import utils
    utils.import_utils()
    print(r_cache.expire(no_prefetch=no_prefetch))
