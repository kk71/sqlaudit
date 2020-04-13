#!/bin/bash -

celery -A task.celery_worker worker -Q $@ -l debug