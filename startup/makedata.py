# Author: kk.Fang(fkfkbill@gmail.com)

from os import path

import click

from models import init_models

# initiate database models/connections

init_models()

import past.mkdata


@click.argument("task_id", type=click.INT, required=True)
@click.option("--schema", help="schema(s) to collect", default=None, type=click.STRING)
@click.option("--q", help="use celery or not", default=True, type=click.BOOL)
def main(task_id, schema, q):
    """manually send a message to queue for running sql analysis"""
    if not task_id:
        print("task_id is required.")
        return
    print(f"task_id={task_id} schema={schema} use_queue={q}")
    past.mkdata.run(task_id, schema, q, operator=path.split(__file__)[1])

