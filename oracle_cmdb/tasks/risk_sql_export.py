import re
import os.path

import xlsxwriter

import settings
import task.const
from task.task import *


@register_task(task.const.TASK_TYPE_RISK_RULE_SQL)
class RiskRuleSqlExport(BaseTask):
    """风险规则SQL导出"""

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        filename: str = kwargs["filename"]
        parame_dict: dict = kwargs["parame_dict"]

        path = os.path.join(settings.EXPORT_DIR, filename)
        wb = xlsxwriter.Workbook(path)

        risk_rule_outer_heads = ["采集时间", "schema名称", "风险分类名称", "风险等级", "扫描得到合计", "一次采集id"]
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
            ws = wb.add_worksheet(re.sub('[*%]', '', r_r_outer['rule_desc'][:20]) + f'-{a}')
            ws.set_row(0, 20, title_format)
            ws.set_column(0, 0, 60)
            ws.set_column(1, 1, 60)
            ws.set_column(2, 2, 60)

            [ws.write(0, x, field, title_format) for x, field in enumerate(risk_rule_sql_inner)]
            row_num += 1
            ws.write(row_num, 0, r_r_outer["etl_date"], content_format)
            ws.write(row_num, 1, r_r_outer["schema"], content_format)
            ws.write(row_num, 2, r_r_outer["rule_desc"], content_format)
            ws.write(row_num, 3, r_r_outer["severity"], content_format)
            ws.write(row_num, 4, r_r_outer["rule_num"], content_format)
            ws.write(row_num, 5, r_r_outer["task_record_id"], content_format)

            rows_nums = 1
            for r_r_s_inner in risk_rule_sql_inner:
                [ws.write(3, x, field, title_format) for x, field in enumerate(risk_rule_sql_inner_heads)]
                if r_r_s_inner['task_record_id'] in list(r_r_outer.values()) and \
                        r_r_s_inner['schema_name'] in list(r_r_outer.values()) and \
                        r_r_s_inner['rule']['rule_name'] in list(r_r_outer.values()):
                    ws.write(3 + rows_nums, 0, r_r_s_inner['sql_id'], content_format)
                    ws.write(3 + rows_nums, 1, r_r_s_inner['sql_text'], content_format)
                    rows_nums += 1
        wb.close()
        return path
