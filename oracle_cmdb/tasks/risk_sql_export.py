import re
import os.path

import xlsxwriter

import settings
import task.const
from task.task import *
from rule.const import RULE_LEVELS_CHINESE
from models.sqlalchemy import make_session


@register_task(task.const.TASK_TYPE_RISK_RULE_SQL)
class RiskRuleSqlExport(BaseTask):
    """风险规则SQL导出"""

    @classmethod
    def report(cls, path_prefix, filename, **kwargs):
        parame_dict: dict = kwargs["parame_dict"]
        path = os.path.join(path_prefix, filename)
        wb = xlsxwriter.Workbook(path)

        risk_rule_outer_heads = ["采集时间", "schema名称", "风险分类名称", "风险等级", "扫描得到合计", "影响", "优化建议", "一次采集id"]
        risk_rule_sql_inner_heads = ["SQL ID", 'SQL_TEXT']

        title_format = wb.add_format({
            'size': 14,
            'bold': 30,
            'align': 'center',
            'valign': 'vcenter',
        })
        content_format = wb.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'size': 11,
            'text_wrap': True,
        })
        a = 0
        risk_rule_outer = parame_dict['risk_rule_outer']
        risk_rule_sql_inner = parame_dict['risk_rule_sql_inner']
        for row_num, r_r_outer in enumerate(risk_rule_outer):
            a += 1
            row_num = 0
            ws = wb.add_worksheet(re.sub('[*%]', '', r_r_outer['rule']['desc'][:20]) + f'-{a}')
            ws.set_row(0, 20, title_format)
            ws.set_column(0, 0, 60)
            ws.set_column(1, 1, 60)
            ws.set_column(2, 2, 60)

            [ws.write(0, x, field, title_format) for x, field in enumerate(risk_rule_outer_heads)]
            row_num += 1
            ws.write(row_num, 0, r_r_outer["create_time"], content_format)
            ws.write(row_num, 1, r_r_outer["schema_name"], content_format)
            ws.write(row_num, 2, r_r_outer["rule"]["desc"], content_format)
            ws.write(row_num, 3, RULE_LEVELS_CHINESE[r_r_outer["level"]], content_format)
            ws.write(row_num, 4, r_r_outer["issue_num"], content_format)
            ws.write(row_num, 5, r_r_outer["rule"]["summary"], content_format)
            ws.write(row_num, 6, "\n".join(r_r_outer["rule"]["solution"]), content_format)
            ws.write(row_num, 7, r_r_outer["task_record_id"], content_format)

            rows_nums = 1
            for r_r_s_inner in risk_rule_sql_inner:
                [ws.write(3, x, field, title_format) for x, field in enumerate(risk_rule_sql_inner_heads)]
                if r_r_s_inner['task_record_id'] in list(r_r_outer.values()) and \
                        r_r_s_inner['schema_name'] in list(r_r_outer.values()) and \
                        r_r_s_inner['rule_name'] in list(r_r_outer['rule'].values()):
                    ws.write(3 + rows_nums, 0, r_r_s_inner['sql_id'], content_format)
                    ws.write(3 + rows_nums, 1, r_r_s_inner['sql_text'], content_format)
                    rows_nums += 1
        wb.close()
        return path

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        filename: str = kwargs.pop("filename")
        from ..restful_api.risk_rule import GetRiskRuleBase, RiskRuleSql
        with make_session() as session:
            risk_rule_outer, cmdb_id, task_record_id_list = \
                GetRiskRuleBase().get_risk_rule(session, **kwargs)
            parame_dict = RiskRuleSql().risk_rule_sql_inner(risk_rule_outer, cmdb_id, task_record_id_list)

            path = cls.report(settings.EXPORT_DIR, filename, parame_dict=parame_dict)
            return path
