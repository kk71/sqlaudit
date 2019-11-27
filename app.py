#!/usr/bin/env python3
# Author: kk.Fang(fkfkbill@gmail.com)

from utils.version_utils import get_versions

__VERSION__ = ".".join([str(i) for i in get_versions()["versions"][-1]["version"]])

from os import path
from utils.datetime_utils import *

print(f"SQL-Audit version {__VERSION__} (process started at {dt_to_str(arrow.now())})")

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
    app.listen(settings.WEB_PORT, settings.WEB_IP)
    print(f"Listening on port {settings.WEB_IP}:{settings.WEB_PORT} ...")
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
@click.option(
    "--filename", help="the json filename", default="./files/rule.json", type=click.STRING)
def export_rules(filename):
    """export rules to a json file"""
    from utils import rule_utils
    print(f"going to export rule to {filename}...")
    all_num = rule_utils.export_rule_to_json_file(filename)
    print(f"Done({all_num}).")


@click.command()
@click.option(
    "--filename", help="the json filename", default="./files/rule.json", type=click.STRING)
def import_rules(filename):
    """import rules from a json file, deduplicated"""
    from utils import rule_utils
    print(f"going to import rule from {filename}...")
    imported_num, all_num = rule_utils.import_from_json_file(filename)
    print(f"Done({imported_num} of {all_num}).")
    # if input("Do you want to set all rules as risk rule? y/n") == "y":
    if True:
        print("going to import risk rules...")
        from models.oracle import make_session
        with make_session() as session:
            r = rule_utils.set_all_rules_as_risk_rule(session)
        print(f"Done({r})")


@click.command()
def delete_rules():
    """delete all rules and risk rules."""
    from models.oracle import make_session, RiskSQLRule
    from models.mongo import Rule
    with make_session() as session:
        n = session.query(RiskSQLRule).delete()
    print(f"deleted {n} risk rules.")
    n = Rule.objects().delete()
    print(f"deleted {n} rules.")


@click.command()
def update_risk_rules():
    """update risk rules with rules"""
    from models.oracle import make_session, RiskSQLRule
    from utils import rule_utils
    with make_session() as session:
        n = session.query(RiskSQLRule).delete()
    print(f"deleted {n} risk rules.")
    with make_session() as session:
        r = rule_utils.set_all_rules_as_risk_rule(session)
    print(f"Done({r})")


@click.command()
@click.argument("task_id", type=click.INT, required=True)
@click.option("--schema", help="schema(s) to collect", default=None, type=click.STRING)
@click.option("--q", help="use celery or not", default=True, type=click.BOOL)
def makedata(task_id, schema, q):
    """manually send a message to queue for running sql analysis"""
    import past.mkdata
    if not task_id:
        print("task_id is required.")
        return
    print(f"task_id={task_id} schema={schema} use_queue={q}")
    past.mkdata.run(task_id, schema, q, operator=path.split(__file__)[1])


@click.command()
@click.argument("filename", required=True, type=click.STRING)
def create_env(filename):
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
@click.option("--q", help="use queue to run", default=True, type=click.BOOL)
@click.option("--no-prefetch", help="do not prefetch", default=False, type=click.BOOL)
def clear_cache(q=True, no_prefetch=False):
    """clear all cache"""
    from task.clear_cache import clear_cache
    if q:
        to_run = clear_cache.delay
    else:
        to_run = clear_cache
    to_run(no_prefetch=no_prefetch)


@click.command()
@click.argument("job_id", type=click.INT, required=True)
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


@click.command()
def gen_license():
    """generate license"""
    from past.utils.product_license import gen_license
    gen_license()


@click.command()
@click.option("--q", type=click.STRING, help="specify a queue name")
def flush_celery_q(q):
    """clear collection queue"""
    from utils.task_utils import flush_celery_q
    if q:
        flush_celery_q(q)
    else:
        flush_celery_q()
    print('done')


@click.command()
def password_convert():
    """warning: this should be run only once!!! For migration only."""
    from hashlib import md5
    from models.oracle import make_session, User
    if input("make sure you're going to convert all users' password to md5.?(y) ") != "y":
        print("aborted.")
        exit()
    with make_session() as session:
        for user in session.query(User):
            user.password = md5(user.password.encode("utf-8")).hexdigest()
            session.add(user)
    print("all changed.")


if __name__ == "__main__":
    cli.add_command(runserver)
    cli.add_command(shell)
    cli.add_command(export_rules)
    cli.add_command(import_rules)
    cli.add_command(update_risk_rules)
    cli.add_command(makedata)
    cli.add_command(create_env)
    cli.add_command(schedule)
    cli.add_command(clear_cache)
    cli.add_command(export_task)
    cli.add_command(create_admin)
    cli.add_command(gen_license)
    cli.add_command(delete_rules)
    cli.add_command(flush_celery_q)
    cli.add_command(password_convert)
    cli()
