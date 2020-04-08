# Author: kk.Fang(fkfkbill@gmail.com)

import click

from models import init_models

# initiate database models/connections

init_models()

from task.utils import flush_celery_q


@click.option("--q", type=click.STRING, help="specify a queue name")
def main(q):
    """clear collection queue"""
    if q:
        flush_celery_q(q)
    else:
        flush_celery_q()
    print('done')

