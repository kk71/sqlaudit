# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from rule.rule_cartridge_utils import *
from rule.rule_cartridge import *
from rule.cmdb_rule_utils import initiate_cmdb_rule


def main():
    """for debugging only"""

    # 先把规则代码更新到规则墨盒
    update_code(compare=False)

    # 把规则墨盒导出
    rule_export(RuleCartridge.DEFAULT_JSON_FILE)

    # 然后把规则墨盒更新到某个测试库上
    initiate_cmdb_rule(cmdb_id=2526)
