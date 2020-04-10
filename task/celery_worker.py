# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

init_models()

from .celery import celery_app as app
from . import celery_collect

# TODO the import sentences should be reserved for celery worker!
