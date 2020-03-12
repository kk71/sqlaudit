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


# === 基础服务相关 ===

@cli.command()
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


@cli.command()
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


@cli.command()
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


@cli.command()
def create_admin():
    """create the admin user"""
    from hashlib import md5
    from models.oracle import User, make_session
    with make_session() as session:
        default_password = "123456"
        admin = User(
            login_user=settings.ADMIN_LOGIN_USER,
            user_name="系统管理员",
            password=md5(default_password.encode("utf-8")).hexdigest())
        session.add(admin)
    print(f"admin user named {settings.ADMIN_LOGIN_USER} created "
          f"with password {default_password}")
    print("* DO NOT FORGET TO CHANGE THE DEFAULT PASSWORD!")


@cli.command()
def gen_license():
    """generate license"""
    from utils.product_license import gen_license
    gen_license()


@cli.command()
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


# === 线上规则相关 ===

@cli.command()
@click.option(
    "--filename", help="the json filename", default="./files/rule.json", type=click.STRING)
def export_rules(filename):
    """export rules to a json file"""
    from utils import rule_utils
    print(f"going to export rule to {filename}...")
    all_num = rule_utils.export_rule_to_json_file(filename)
    print(f"Done({all_num}).")


@cli.command()
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


@cli.command()
def delete_rules():
    """delete all rules and risk rules."""
    from models.oracle import make_session, RiskSQLRule
    from models.mongo import Rule
    with make_session() as session:
        n = session.query(RiskSQLRule).delete()
    print(f"deleted {n} risk rules.")
    n = Rule.objects().delete()
    print(f"deleted {n} rules.")


@cli.command()
def update_risk_rules():
    """update risk rules with rules"""
    from models.oracle import make_session, RiskSQLRule
    from utils import rule_utils
    with make_session() as session:
        n = session.query(RiskSQLRule).delete(synchronize_session=False)
    print(f"deleted {n} risk rules.")
    with make_session() as session:
        r = rule_utils.set_all_rules_as_risk_rule(session)
    print(f"Done({r})")


# === 采集分析相关 ===

@cli.command()
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


@cli.command()
def schedule():
    """start a task scheduler"""
    import task.schedule
    task.schedule.main()


@cli.command()
@click.argument("job_id", type=click.INT, required=True)
def export_task(job_id):
    """export html report"""
    if not job_id:
        print("job_id is required.")
        return
    from task.export import export
    export(job_id)


@cli.command()
@click.option("--q", type=click.STRING, help="specify a queue name")
def flush_celery_q(q):
    """clear collection queue"""
    from utils.task_utils import flush_celery_q
    if q:
        flush_celery_q(q)
    else:
        flush_celery_q()
    print('done')


# === 工单规则相关 ===

@cli.command()
@click.option(
    "--filename",
    help="the json filename",
    default="./files/ticket-rule.json",
    type=click.STRING)
def ticket_rule_import(filename):
    """import ticket rules, deduplicated."""
    from new_rule.export_utils import ticket_rule_import
    print(f"going to import ticket rules from {filename} ...")
    imported_num = ticket_rule_import(filename)
    print(f"{imported_num} rule(s) imported.")


@cli.command()
@click.option(
    "--compare",
    help="don't import, compare only.",
    default=False,
    type=click.BOOL)
def ticket_rule_import_code(compare: bool):
    """FOR DEVELOPMENT: import rule code from ticket_rules"""
    from pathlib import Path
    import settings
    from new_rule.rule import TicketRule
    if compare:
        print("=== compare only ===")
    different_codes = []
    not_imported_rules = []
    for tr in TicketRule.objects().all():
        try:
            code_file = Path(settings.SETTINGS_FILE_DIR) / \
                        f"ticket-rules/{tr.db_type}/" \
                        f"{tr.analyse_type.lower()}/{tr.name}.py"
            if not code_file.exists():
                raise Exception(f"code file {code_file} not existed.")
            if not code_file.is_file():
                raise Exception(f"{code_file} is not a file.")
            with open(code_file, "r") as z:
                new_code = z.read()
                if tr.code != new_code:
                    different_codes.append(tr.unique_key())
                    if not compare:
                        tr.code = new_code
                        tr.analyse(test_only=True)
                        tr.save()
        except Exception as e:
            print(e)
            not_imported_rules.append(str(tr))
    if not compare:
        print(f"{len(not_imported_rules)} rules not updated "
              f"due to local code file not found: {not_imported_rules}")
        print(f"{len(different_codes)} rules updated in code: {different_codes}")
    else:
        print(f"{len(different_codes)} rules different in code "
              f"and local code files: {different_codes}")


@cli.command()
@click.option(
    "--filename",
    help="the json filename",
    default="./files/ticket-rule.json",
    type=click.STRING)
def ticket_rule_export(filename):
    """export ticket rules, target json file will be overwritten if existed."""
    from new_rule.export_utils import ticket_rule_export
    print(f"going to export ticket rules to {filename} ...")
    exported_num = ticket_rule_export(filename)
    print(f"{exported_num} rule(s) exported.")


@cli.command()
def ticket_rule_export_code():
    """FOR DEVELOPMENT: export rule code to ticket_rules"""
    return


@cli.command()
def ticket_rule_drop():
    """delete all ticket rules, use with caution!"""
    from new_rule.export_utils import ticket_rule_drop
    dropped_num = ticket_rule_drop()
    print(f"{dropped_num} ticket rule(s) dropped.")


if __name__ == "__main__":
    cli()
