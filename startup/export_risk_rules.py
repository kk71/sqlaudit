
import click

from models import init_models

# initiate database models/connections

init_models()

from utils.risk_rule_utils import export_risk_rule_to_json_file


@click.option("--filename", help="the json filename", default="./files/risk_rules.json", type=click.STRING)
def main(filename):
    """export risk rules to a json file"""
    print(f"going to export risk rules to {filename}...")
    all_num=export_risk_rule_to_json_file(filename)
    print(f"Done({all_num}).")



