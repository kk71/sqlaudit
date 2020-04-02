# Author: kk.Fang(fkfkbill@gmail.com)

from IPython import embed

from models import init_models

# initiate database models/connections

init_models()


def main():
    """open an iPython shell to perform some tasks"""

    # this shall be execute AFTER the init_models run
    # this is for fast model referring
    # this session object is auto-commit and auto-flush enabled.
    embed(header='''SQL-Audit shell for debugging is now running.''')


