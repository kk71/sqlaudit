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
    if input("Do you want to set all rules as risk rule? y/n") == "y":
        from models.oracle import make_session
        with make_session() as session:
            rule_utils.set_all_rules_as_risk_rule(session)
        print("done")


@click.command()
@click.option("--task_id", help="")
@click.option("--schema", help="", default=None)
@click.option("--q", help="use celery or not", default=0)
def makedata(task_id, schema, q):
    """manually send a message to queue for running sql analysis"""
    import past.mkdata
    if not task_id:
        print("task_id is required.")
        return
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


@click.command()
@click.option("--use-q", help="use queue to run")
@click.option("--no-prefetch", help="do not prefetch")
def clear_cache(use_q=True, no_prefetch=False):
    """clear all cache"""
    from task.clear_cache import clear_cache
    if use_q:
        to_run = clear_cache.delay
    else:
        to_run = clear_cache
    if no_prefetch == "0":
        no_prefetch = False
    elif no_prefetch == "1":
        no_prefetch = True
    else:
        assert 0
    to_run(no_prefetch=no_prefetch)


@click.command()
@click.option("--job_id")
def export_task(job_id):
    """export html report"""
    if not job_id:
        print("job_id is required.")
        return
    from task.export import export
    export(job_id)


@click.command()
def create_admin():
    """create an admin user"""
    from models.oracle import User, make_session
    with make_session() as session:
        default_password = "123456"
        admin = User(
            login_user=settings.ADMIN_LOGIN_USER,
            user_name="系统管理员",
            password=default_password)
        session.add(admin)
    print(f"* admin user named {settings.ADMIN_LOGIN_USER} created with password {default_password}")


if __name__ == "__main__":
    cli.add_command(runserver)
    cli.add_command(shell)
    cli.add_command(importrules)
    cli.add_command(makedata)
    cli.add_command(createenv)
    cli.add_command(schedule)
    cli.add_command(clear_cache)
    cli.add_command(export_task)
    cli.add_command(create_admin)
    cli()
