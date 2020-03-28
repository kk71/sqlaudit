# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models

# initiate database models/connections

init_models()

from models.oracle import make_session, RiskSQLRule
from models.mongo import Rule


def main():
    """delete all rules and risk rules."""
    with make_session() as session:
        n = session.query(RiskSQLRule).delete()
    print(f"deleted {n} risk rules.")
    n = Rule.objects().delete()
    print(f"deleted {n} rules.")



