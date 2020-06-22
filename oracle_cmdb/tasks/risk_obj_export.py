
import re
import os.path

import xlsxwriter

import settings
import task.const
from task.task import *
from rule.const import RULE_LEVELS_CHINESE
from models.sqlalchemy import make_session


@register_task(task.const.TASK_TYPE_RISK_RULE_OBJ)
class RiskRuleObjExport(BaseTask):
    """风险规则对象导出"""

    @classmethod
    def report(cls, path_prefix, filename, **kwargs):
        params: dict = kwargs["parame_dict"]
        from ..restful_api.risk_rule import GetRiskRuleBase, RiskRuleObj
        with make_session() as session:
            risk_rule_outer, cmdb_id, task_record_id_list=\
                GetRiskRuleBase().get_risk_rule(session,**params)
            risk_rule_obj_inner=RiskRuleObj().get_risk_obj(cmdb_id=cmdb_id,task_record_id_in=task_record_id_list)


            path = os.path.join(path_prefix, filename)
            wb = xlsxwriter.Workbook(path)

            risk_rule_outer_heads = ["采集时间", "schema名称", "风险分类名称", "风险等级", "扫描得到合计", "影响", "优化建议", "一次采集id"]
            risk_rule_obj_inner_heads = ['对象名称', '对象类型', '风险问题']

            title_format = wb.add_format({
                'size': 14,
                'bold': 30,
                'align': 'center',
                'valign': 'vcenter',
            })
            content_format = wb.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'size': 13,
                'text_wrap': True,
            })
            a = 0

            for row_num, r_r_outer in enumerate(risk_rule_outer):
                a += 1
                row_num = 0
                ws = wb.add_worksheet(re.sub('[*%]', '', r_r_outer["rule"]["desc"][:20]) + f'-{a}')
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
                for r_r_o_inner in risk_rule_obj_inner:
                    [ws.write(3, x, field, title_format) for x, field in enumerate(risk_rule_obj_inner_heads)]
                    if r_r_o_inner['task_record_id'] in list(r_r_outer.values()) and \
                            r_r_o_inner['schema_name'] in list(r_r_outer.values()) and \
                            r_r_o_inner['rule_name'] in list(r_r_outer['rule'].values()):
                        ws.write(3 + rows_nums, 0, r_r_o_inner['object_name'], content_format)
                        ws.write(3 + rows_nums, 1, r_r_o_inner['object_type'], content_format)
                        ws.write(3 + rows_nums, 2, r_r_o_inner['issue_description_str'], content_format)
                        rows_nums += 1
            wb.close()
            return path

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        filename: str = kwargs.pop("filename")

        path = cls.report(settings.EXPORT_DIR, filename, **kwargs)
        return path