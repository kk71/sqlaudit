#!/usr/bin/env python3
# Author: kk.Fang(fkfkbill@gmail.com)

__VERSION__ = "0.2.0"
print(f"SQL-Audit version {__VERSION__}")

import os

import click
from tornado.web import Application
import tornado.ioloop
from tornado.log import enable_pretty_logging

import settings
from backend.models import init_models


# initiate database models/connections

init_models()


@click.group()
def cli():
    pass


@click.command()
def runserver():
    """start a web server for sqlaudit backend"""
    print("Starting http server for backend...")
    from backend.urls import urls
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
    from backend.models import engine
    # this is for fast model referring
    import backend.models.oracle as o
    # this session object is auto-commit and auto-flush enabled.
    Session = sessionmaker(
        bind=engine,
        autocommit=True,
        autoflush=True
    )
    ss = Session()
    embed(header='''SQL-Audit shell for debugging is now running.
When operating oracle, no need to use backend.models.oracle.utils.make_session,
a session object with both autocommit and autoflush on is created named ss.
                 ''')


@click.command()
@click.option("--filename", help="the json filename", default="./files/rule.json")
def importrules(filename):
    """import rules from a json file, NOT deduplicate"""
    from backend.utils import rule_utils
    print(f"going to import rule from {filename}...")
    imported_num, all_num = rule_utils.import_from_json_file(filename)
    print(f"Done({imported_num} of {all_num}).")


@click.command()
def makedata():
    """manually send a message to queue for running sql analysis"""
    return


@click.command()
@click.option("--filename", help="the filename")
def createenv(filename):
    """create py.env file with default values"""
    print(f"going to create a new env file to {filename}...")
    with open(filename, "w") as z:
        z.writelines([f"{i[0]}={i[1]}\n" for i in settings.ALL_ENV_VARS])
    print("Done.")


if __name__ == "__main__":
    cli.add_command(runserver)
    cli.add_command(shell)
    cli.add_command(importrules)
    cli.add_command(makedata)
    cli.add_command(createenv)
    cli()
