# Author: kk.Fang(fkfkbill@gmail.com)

__ALL__ = [
    "celery"
]

from celery import Celery

# initiate celery application
# usage: wherever you want add task, import celery below

celery = Celery(__name__)
celery.config_from_object("celery_conf")

