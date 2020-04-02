# Author: kk.Fang(fkfkbill@gmail.com)

from pathlib import Path

import click

import settings
from models import init_models

# initiate database models/connections

init_models()

from rule.rule import TicketRule


@click.option(
    "--compare",
    help="don't import, compare only.",
    default=False,
    type=click.BOOL)
def main(compare: bool):
    """FOR DEVELOPMENT: import rule code from ticket_rules"""
    if compare:
        print("=== compare only ===")
    different_codes = []
    not_imported_rules = []
    for tr in TicketRule.objects().all():
        try:
            code_file = Path(settings.SETTINGS_FILE_DIR) / \
                        f"new_rule/ticket-rules/{tr.db_type}/" \
                        f"{tr.name}.py"
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
                        tr.test()
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

