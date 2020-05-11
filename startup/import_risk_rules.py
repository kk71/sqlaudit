
import click

from models import init_models

# initiate database models/connections

init_models()

from utils.risk_rule_utils import import_from_risk_rule_json_file


@click.option("--filename", help="the json filename", default="./files/risk_rules.json", type=click.STRING)
def main(filename):
    """import risk rules json file to oracle
    and ---- adjust rule weights and scores according to the level"""
    print(f"going to import rule from {filename}...")
    import_num_risks_rules,import_num_rules=import_from_risk_rule_json_file(filename)
    print(f"Done,risk_rules:({import_num_risks_rules})")
    print(f"Done,rules:({import_num_rules})")
