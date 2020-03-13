# Author: kk.Fang(fkfkbill@gmail.com)

from hashlib import md5

from models import init_models

# initiate database models/connections

init_models()

from models.oracle import make_session, User


def main():
    """warning: this should be run only once!!! For migration only."""
    if input("make sure you're going to convert all users' password to md5.?(y) ") != "y":
        print("aborted.")
        exit()
    with make_session() as session:
        for user in session.query(User):
            user.password = md5(user.password.encode("utf-8")).hexdigest()
            session.add(user)
    print("all changed.")



