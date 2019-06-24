# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from task.base import *
from utils.perf_utils import r_cache
import utils


@celery.task
def clear_cache():
    utils.import_utils()
    print(r_cache.expire())
