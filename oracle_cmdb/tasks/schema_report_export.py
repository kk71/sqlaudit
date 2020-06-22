import re
import os.path

import xlsxwriter

import settings
import task.const
from task.task import *
from rule.const import RULE_LEVELS_CHINESE


@register_task(task.const.TASK_TYPR_SCHEMA_REPORT)
class SchemaReportExport(BaseTask):
    """健康中心schema报告导出"""

    @classmethod
    def report(cls,path_prefix, filename, **kwargs):
        parame_dict: dict = kwargs["parame_dict"]
        from ..restful_api.health_center import SchemaIssueRuleBase, OutputDataBase
        schema_issue_rule_dict, cmdb_id, task_record_id, schema_name=\
            SchemaIssueRuleBase().schema_issue_rule(**parame_dict)
        output_data=OutputDataBase().get_output_data(cmdb_id,task_record_id,schema_name)

        path = os.path.join(path_prefix, filename)
        wb = xlsxwriter.Workbook(path)
        format_title = wb.add_format({
            'bold': 1,
            'size': 14,
            'align': 'center',
            'valign': 'vcenter',

        })
        format_text = wb.add_format({
            'valign': 'vcenter',
            'align': 'center',
            'size': 14,
            'text_wrap': True,
        })
        # 上栏
        heads = ["连接名称", "SCHEMA", "创建时间", "schema分数"]
        connect_name = schema_issue_rule_dict["connect_name"]
        schema_name = schema_issue_rule_dict["schema_name"]
        create_time = schema_issue_rule_dict["create_time"]
        schema_score = schema_issue_rule_dict["schema_score"]['entry_score']['ONLINE']
        heads_data = [connect_name, schema_name, create_time, schema_score]
        # 中栏
        schema_issue_rule_heads = ['规则名称', '规则描述', '风险等级', '违反次数']
        rule_issues = schema_issue_rule_dict["rule_issue"]
        # 下栏
        output_data = output_data

        a = 0
        for rule_issue in rule_issues:
            a += 1
            rule_ws = wb.add_worksheet(re.sub('[*%]', '', rule_issue["rule_desc"] + f'-{a}'))

            rule_ws.set_column(0, 0, 40)
            rule_ws.set_column(1, 1, 110)
            rule_ws.set_column(2, 2, 30)
            rule_ws.set_column(3, 6, 30)

            [rule_ws.write(0, x, field, format_title) for x, field in enumerate(heads)]
            [rule_ws.write(1, x, field, format_text) for x, field in enumerate(heads_data)]

            [rule_ws.write(3, x, field, format_title) for x, field in enumerate(schema_issue_rule_heads)]
            rule_ws.write(4, 0, rule_issue['rule_name'], format_text)
            rule_ws.write(4, 1, rule_issue['rule_desc'], format_text)
            rule_ws.write(4, 2, RULE_LEVELS_CHINESE[rule_issue['level']], format_text)
            rule_ws.write(4, 3, rule_issue['issue_num'], format_text)

            i = 7
            for r_o_d in output_data:  # [{"rule_name":{output_data}},{},{}]
                for rule_name, o_d in r_o_d.items():
                    if rule_name == rule_issue['rule_name']:
                        [rule_ws.write(6, x, field, format_text)
                         for x, field in enumerate(list(o_d.keys()))]
                        [rule_ws.write(i, x, "\n".join(field)
                        if isinstance(field, list) else field, format_text)
                         for x, field in enumerate(list(o_d.values()))]
                        i += 1
        wb.close()
        return path

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        filename: str = kwargs.pop("filename")

        path = cls.report(settings.HEALTH_DIR,filename, **kwargs)
        return path