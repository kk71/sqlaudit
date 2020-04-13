# Author: kk.Fang(fkfkbill@gmail.com)

from . import celery_conf, modules, celery


modules.collect_dynamic_modules(celery_conf.PACKAGES_TO_SEARCH_FOR_TASKS)

print(celery_conf.imports)
print(celery_conf.task_queues)
print(celery_conf.task_routes)

celery.celery_app.config_from_object(celery_conf)
all_tasks = " - "
if celery.celery_app.tasks:
    all_tasks = ", ".join(celery.celery_app.tasks)
print("all tasks: \n" + all_tasks)

