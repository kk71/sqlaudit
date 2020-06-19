# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "TicketExport"
]

import os.path

import xlsxwriter

import settings
import task.const
from task.task import *


@register_task(task.const.TASK_TYPE_TICKET_EXPORT)
class TicketExport(BaseTask):
    """导出工单"""

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        filename: str = kwargs["filename"]
        parame_dict: dict = kwargs["parame_dict"]

        # 第一个shell,工单详情、子工单统计
        path = os.path.join(settings.EXPORT_DIR, filename)
        wb = xlsxwriter.Workbook(path)
        ws = wb.add_worksheet('工单详情')
        ws.set_column(0, 2, 20)
        ws.set_column(3, 4, 18)
        ws.set_column(5, 6, 20)
        ws.set_column(6, 8, 18)
        ws.set_column(9, 9, 50)
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
        format_top = wb.add_format({
            'align': 'left',
            'valign': 'vcenter',
            'text_wrap': True,
            'color': 'red',
            # 'bg_color': 'yellow'
        })
        ticket_heads = parame_dict['ticket_heads']
        [ws.write(0, x, field.upper(), format_title) for x, field in enumerate(ticket_heads)]
        the_ticket = parame_dict['the_ticket']
        ws.write(1, 0, the_ticket['ticket_id'], format_text)
        ws.write(1, 1, the_ticket['task_name'], format_text)
        ws.write(1, 2, the_ticket['db_type'], format_text)
        ws.write(1, 3, the_ticket['cmdb_id'], format_text)
        ws.write(1, 4, the_ticket['schema_name'], format_text)
        ws.write(1, 5, the_ticket['script_sum'], format_text)
        ws.write(1, 6, the_ticket['sub_ticket_count'], format_text)
        ws.write(1, 7, the_ticket['score'], format_text)
        ws.write(1, 8, the_ticket['submit_time'], format_text)
        ws.write(1, 9, the_ticket['submit_owner'], format_text)
        ws.write(1, 10, the_ticket['status'], format_text)
        ws.write(1, 11, the_ticket['audit_time'], format_text)
        ws.write(1, 12, the_ticket['audit_owner'], format_text)
        ws.write(1, 13, the_ticket['audit_comments'], format_text)

        sub_ticket_stats_heads = parame_dict['sub_ticket_stats_heads']
        sub_ticket_stats = parame_dict['sub_ticket_stats']
        [ws.write(3, x, field.upper(), format_title) for x, field in enumerate(sub_ticket_stats_heads)]
        ws.write(4, 0, sub_ticket_stats[0], format_text)
        ws.write(4, 1, sub_ticket_stats[1], format_top)
        ws.write(4, 2, sub_ticket_stats[2], format_text)
        ws.write(4, 3, sub_ticket_stats[3], format_text)

        # 第二个shell,获得静态问题子工单
        static_ws = wb.add_worksheet('静态问题子工单')
        static_ws.set_column(0, 1, 25)
        static_ws.set_column(1, 3, 70)

        issue_static_sub_ticket_heads = parame_dict['issue_static_sub_ticket_heads']
        issue_static_sub_ticket = parame_dict['issue_static_sub_ticket']
        [static_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(issue_static_sub_ticket_heads)]
        for row_num, row in enumerate(issue_static_sub_ticket):
            static_ws.write(row_num + 1, 0, row[0], format_text)
            static_ws.write(row_num + 1, 1, row[1], format_text)
            static_ws.write(row_num + 1, 2, row[2], format_text)
            static_ws.write(row_num + 1, 3, row[3], format_text)
            static_ws.write(row_num + 1, 4, row[4], format_text)
            static_ws.write(row_num + 1, 5, row[5], format_text)

        # 第三个shell,获得动态问题子工单
        dynamic_ws = wb.add_worksheet('动态问题子工单')
        dynamic_ws.set_column(0, 1, 25)
        dynamic_ws.set_column(1, 3, 70)

        issue_dynamic_sub_ticket_heads = parame_dict['issue_dynamic_sub_ticket_heads']
        issue_dynamic_sub_ticket = parame_dict['issue_dynamic_sub_ticket']

        [dynamic_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(issue_dynamic_sub_ticket_heads)]
        for row_num, row in enumerate(issue_dynamic_sub_ticket):
            dynamic_ws.write(row_num + 1, 0, row[0], format_text)
            dynamic_ws.write(row_num + 1, 1, row[1], format_text)
            dynamic_ws.write(row_num + 1, 2, row[2], format_text)
            dynamic_ws.write(row_num + 1, 3, row[3], format_text)
            dynamic_ws.write(row_num + 1, 4, row[4], format_text)
            dynamic_ws.write(row_num + 1, 5, row[5], format_text)

        # 第四个shell,获得所有动静态问题子工单
        all_ws = wb.add_worksheet('所有动静态问题子工单')
        all_ws.set_column(0, 1, 25)
        all_ws.set_column(1, 4, 70)
        all_issue_sub_ticket_heads = parame_dict['all_issue_sub_ticket_heads']
        all_issue_sub_ticket = parame_dict['all_issue_sub_ticket']

        [all_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(all_issue_sub_ticket_heads)]
        for row_num, row in enumerate(all_issue_sub_ticket):
            all_ws.write(row_num + 1, 0, row[0], format_text)
            all_ws.write(row_num + 1, 1, row[1], format_text)
            all_ws.write(row_num + 1, 2, row[2], format_text)
            all_ws.write(row_num + 1, 3, row[3], format_text)
            all_ws.write(row_num + 1, 4, row[4], format_text)
            all_ws.write(row_num + 1, 5, row[5], format_text)
            all_ws.write(row_num + 1, 6, row[6], format_text)

        wb.close()
        return path
