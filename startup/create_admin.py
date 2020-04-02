# Author: kk.Fang(fkfkbill@gmail.com)

from hashlib import md5

import settings
from models import init_models

# initiate database models/connections

init_models()

from models.sqlalchemy import make_session


def main():
    """create the admin user"""
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

