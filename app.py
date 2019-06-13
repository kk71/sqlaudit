#!/usr/bin/env python3
# Author: kk.Fang(fkfkbill@gmail.com)

__VERSION__ = "0.2.0"

from utils.datetime_utils import *
print(f"SQL-Audit version {__VERSION__} ({dt_to_str(arrow.now())})")

import click
from tornado.web import Application
import tornado.ioloop
from tornado.log import enable_pretty_logging

import settings
from models import init_models


# initiate database models/connections

init_models()


@click.group()
def cli():
    pass


@click.command()
def runserver():
    """start a web server for sqlaudit restful_api"""
    print("Starting http server for restful api...")
    from restful_api.urls import urls
    app = Application(
        urls,
        debug=settings.DEBUG,
        autoreload=True,
    )
    enable_pretty_logging()
    app.listen(settings.WEB_PORT)
    print(f"Listening on port {settings.WEB_PORT} ...")
    tornado.ioloop.IOLoop.instance().start()


@click.command()
def shell():
    """open an iPython shell to perform some tasks"""
    from IPython import embed
    from sqlalchemy.orm import sessionmaker

    # this shall be execute AFTER the init_models run
    from models import engine
    # this is for fast model referring
    # this session object is auto-commit and auto-flush enabled.
    Session = sessionmaker(
        bind=engine,
        autocommit=True,
        autoflush=True
    )
    ss = Session()
    import models.oracle as mo
    import models.mongo as mm
    embed(header='''SQL-Audit shell for debugging is now running.
When operating oracle, no need to use restful_api.models.oracle.utils.make_session,
a session object with both autocommit and autoflush on is created named ss.
                 ''')


@click.command()
@click.option("--filename", help="the json filename", default="./files/rule.json")
def importrules(filename):
    """import rules from a json file, deduplicated"""
    from utils import rule_utils
    print(f"going to import rule from {filename}...")
    imported_num, all_num = rule_utils.import_from_json_file(filename)
    print(f"Done({imported_num} of {all_num}).")


@click.command()
@click.option("--task_id", help="")
@click.option("--schema", help="", default=None)
@click.option("--q", help="use celery or not", default=0)
def makedata(task_id, schema, q):
    """manually send a message to queue for running sql analysis"""
    import past.mkdata
    if not task_id:
        print("task_id is required.")
        exit()
    q = int(q)
    q = True if q else False
    print(f"task_id={task_id} schema={schema} use_queue={q}")
    past.mkdata.run(task_id, schema, q)


@click.command()
@click.option("--filename", help="the filename")
def createenv(filename):
    """create py.env file with default values"""
    print(f"going to create a new env file to {filename}...")
    with open(filename, "w") as z:
        z.writelines([f"{i[0]}={i[1]}\n" for i in settings.ALL_ENV_VARS])
    print("Done.")


@click.command()
def schedule():
    """start a task scheduler"""
    import task.schedule
    task.schedule.main()


if __name__ == "__main__":
    cli.add_command(runserver)
    cli.add_command(shell)
    cli.add_command(importrules)
    cli.add_command(makedata)
    cli.add_command(createenv)
    cli.add_command(schedule)
    cli()
