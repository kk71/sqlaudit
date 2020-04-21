# Author: kk.Fang(fkfkbill@gmail.com)

import traceback

import click

from models import init_models

# initiate database models/connections

init_models()

from rule.rule_cartridge import RuleCartridge


@click.option(
    "--compare",
    help="don't import, compare only.",
    default=False,
    type=click.BOOL)
def main(compare: bool):
    """FOR DEVELOPMENT: import rule code to rule cartridge"""
    if compare:
        print("=== compare only ===")
    different_codes = []
    not_imported_rules = []
    for tr in RuleCartridge.objects().all():
        try:
            code_file = RuleCartridge.CODE_FILES_DIR / f"{tr.db_type}/{tr.name}.py"
            if not code_file.exists():
                raise Exception(f"code file {code_file} not existed.")
            if not code_file.is_file():
                raise Exception(f"{code_file} is not a file.")
            with open(str(code_file), "r") as z:
                new_code = z.read()
                if tr.code != new_code:
                    different_codes.append(tr.unique_key())
                    if not compare:
                        tr.code = new_code
                        tr.test()
                        tr.save()
        except Exception as e:
            print(traceback.format_exc())
            not_imported_rules.append(str(tr))
    if not compare:
        print(f"{len(different_codes)} rules updated in code: {different_codes}")
        print(f"{len(not_imported_rules)} rules not updated "
              f"with exceptions above: {not_imported_rules}")
    else:
        print(f"{len(different_codes)} rules different in code "
              f"and local code files: {different_codes}")

