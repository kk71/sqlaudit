# Author: kk.Fang(fkfkbill@gmail.com)

from celery import Celery

from . import celery_conf

celery_app = Celery(__name__)
celery_app.config_from_object(celery_conf)

