# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

import cmdb.const
from rule.rule_cartridge_utils import *
from rule.rule_cartridge import *
from rule.cmdb_rule_utils import initiate_cmdb_rule
from oracle_cmdb.issue.base import *
from models.sqlalchemy import *
from oracle_cmdb.cmdb import *
from cmdb.cmdb_task import *


def main():
    """for debugging only"""

    # 最后运行一下分析任务
    cmdb_task_id = 1991
    task_record_id = 36
    cmdb_id = 2526

    # 先把规则代码更新到规则墨盒
    update_code(compare=False)

    # 把规则墨盒导出
    rule_export(RuleCartridge.DEFAULT_JSON_FILE)

    # 然后把规则墨盒更新到某个测试库上
    initiate_cmdb_rule(cmdb_id=cmdb_id)

    # 检查输出参数和规则是否符合
    for m in OracleOnlineIssue.COLLECTED:
        m.check_rule_output_and_issue(cmdb_id=cmdb_id)

    with make_session() as session:
        the_cmdb_task = session.query(CMDBTask).filter_by(
            id=cmdb_task_id).first()
        the_cmdb = session.query(OracleCMDB).filter_by(
            cmdb_id=the_cmdb_task.cmdb_id).first()
        cmdb_id = the_cmdb.cmdb_id
        schemas: [str] = the_cmdb.get_bound_schemas(session)
        print(f"{len(schemas)} schema(s) to run: {schemas}")

        OracleOnlineIssue.collect()
        OracleOnlineIssue.process(
            cmdb_id=cmdb_id,
            task_record_id=task_record_id,
            schemas=the_cmdb.get_bound_schemas(session)
        )
