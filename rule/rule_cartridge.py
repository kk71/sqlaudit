# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "RuleCartridge"
]

from os import path
from pathlib import Path

from mongoengine import StringField

import cmdb.const
from rule.rule import BaseRule


class RuleCartridge(BaseRule):
    """规则墨盒"""

    db_model = StringField(
        required=True, null=False, choices=cmdb.const.ALL_DB_MODEL)

    UNIQUE_KEYS = ("db_type", "db_model", "name")

    DEFAULT_JSON_FILE: str = str(
        Path(path.dirname(path.realpath(__file__))) / "files/rule.json")

    CODE_FILES_DIR: Path = Path(path.dirname(path.realpath(__file__))) / "rule-code"

    meta = {
        "collection": "rule_cartridge",
        "indexes": [
            {'fields': UNIQUE_KEYS, 'unique': True},
            *UNIQUE_KEYS
        ]
    }
