# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from models.sqlalchemy import *
from rule.rule_cartridge_utils import *
from rule.rule_cartridge import *
from rule.cmdb_rule_utils import initiate_cmdb_rule
from oracle_cmdb.issue.base import *
from oracle_cmdb.cmdb import *


def main():
    """for debugging only"""

    # TODO 给一个测试库验证用
    cmdb_id = 2526
    task_record_id = 126

    # 先把规则代码更新到规则墨盒
    update_code(compare=False)

    # 把规则墨盒导出
    rule_export(RuleCartridge.DEFAULT_JSON_FILE)

    # 然后把规则墨盒更新到某个测试库上
    initiate_cmdb_rule(cmdb_id=cmdb_id)

    # 检查导入的规则墨盒的代码是否有问题(语法，import是否有问题)
    for a_rule in RuleCartridge.filter():
        a_rule.test()

    # 检查输出参数和规则是否符合
    for m in OracleOnlineIssue.COLLECTED:
        m.check_rule_output_and_issue(cmdb_id=cmdb_id)

    # from oracle_cmdb.statistics import OracleBaseStatistics, OracleStatsDashboardDrillDown
    # OracleBaseStatistics.collect()
    # with make_session() as session:
    #     the_cmdb = session.query(OracleCMDB).filter_by(cmdb_id=cmdb_id).first()
    #     schemas: [str] = the_cmdb.get_bound_schemas(session)
    # OracleBaseStatistics.process(
    #     collected=[OracleStatsDashboardDrillDown],
    #     cmdb_id=cmdb_id,
    #     task_record_id=task_record_id,
    #     schemas=schemas
    # )
