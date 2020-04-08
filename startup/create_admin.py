# Author: kk.Fang(fkfkbill@gmail.com)

import settings
from models import init_models

# initiate database models/connections

init_models()

from models.sqlalchemy import make_session
from auth.user import User


def main():
    """create the admin user"""
    with make_session() as session:
        default_password = "123456"
        admin = User(
            login_user=settings.ADMIN_LOGIN_USER,
            username="系统管理员"
        )
        admin.set_password(default_password)
        session.add(admin)
    print(f"admin user named {settings.ADMIN_LOGIN_USER} created "
          f"with password {default_password}")
    print("* DO NOT FORGET TO CHANGE THE DEFAULT PASSWORD!")

