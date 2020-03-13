# Author: kk.Fang(fkfkbill@gmail.com)

from IPython import embed
from sqlalchemy.orm import sessionmaker

from models import init_models

# initiate database models/connections

init_models()

from models import engine


def main():
    """open an iPython shell to perform some tasks"""

    # this shall be execute AFTER the init_models run
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


