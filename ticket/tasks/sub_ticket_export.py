
import os.path
import xlsxwriter

import settings
import task.const
from task.task import *


@register_task(task.const.TASK_TYPE_SUB_TICKET_EXPORT)
class SubTicketExport(BaseTask):
    """导出子工单"""

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        filename: str = kwargs["filename"]
        sub_ticket_q: dict = kwargs["parame_dict"]

        path = os.path.join(settings.EXPORT_DIR, filename)
        wb = xlsxwriter.Workbook(path)
        ws = wb.add_worksheet('子工单详情')
        ws.set_column(0, 2, 20)
        ws.set_column(3, 4, 18)
        ws.set_column(5, 5, 10)
        ws.set_column(6, 6, 50)
        ws.set_row(0, 30)
        format_title = wb.add_format({
            'bold': 1,
            'size': 14,
            'align': 'center',
            'valign': 'vcenter',

        })
        format_text = wb.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'text_wrap': True,
        })
        sub_ticket_heads = ["工单编号","脚本名称", "SQL文本", "静态检测结果", "动态检测结果", "上线状态", "错误信息"]
        [ws.write(0, x, field.upper(), format_title) for x, field in enumerate(sub_ticket_heads)]


        for row_num, sub_ticket in enumerate(sub_ticket_q.all()):
            row_num += 1
            ws.write(row_num, 0, sub_ticket.task_name, format_text)
            ws.write(row_num, 1, sub_ticket.script.script_name, format_text)
            ws.write(row_num, 2, sub_ticket.sql_text, format_text)
            ws.write(row_num, 3, "\n".join(
                [x['rule_desc'] for x in sub_ticket.static]),
                     format_text)
            ws.write(row_num, 4, "\n".join(
                [x["rule_desc"] for x in sub_ticket.dynamic]),
                     format_text)
            ws.write(row_num, 5, sub_ticket.online_status, format_text)
            ws.write(row_num, 6, sub_ticket.error_msg, format_text)
        wb.close()
        return path