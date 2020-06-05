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

        path = os.path.join(settings.EXPORT_DIR, filename)
        wb = xlsxwriter.Workbook(path)
        ws = wb.add_worksheet('任务工单详情')
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
        works_heads = parame_dict['work_list_heads']
        [ws.write(0, x, field.upper(), format_title) for x, field in enumerate(works_heads)]

        works_data = parame_dict['work_list_data']
        ws.write(1, 0, works_data[0], format_text)
        ws.write(1, 1, works_data[1], format_text)
        ws.write(1, 2, works_data[2], format_text)
        ws.write(1, 3, works_data[3], format_text)
        ws.write(1, 4, works_data[4], format_text)
        ws.write(1, 5, works_data[5], format_text)
        ws.write(1, 6, works_data[6], format_text)
        ws.write(1, 7, works_data[7], format_text)
        ws.write(1, 8, works_data[8], format_text)
        ws.write(1, 9, works_data[9], format_text)
        ws.write(1, 10, works_data[10], format_text)
        ws.write(1, 11, works_data[11], format_text)
        ws.write(1, 12, works_data[12], format_text)
        ws.write(1, 13, works_data[13], format_text)
        ws.write(1, 14, works_data[14], format_text)
        ws.write(1, 15, works_data[18], format_text)

        # 同一个sheet里面 统计工单数量
        fail_data = parame_dict['fail_data']
        fail_heads = parame_dict['fail_heads']
        [ws.write(3, x, field.upper(), format_title) for x, field in enumerate(fail_heads)]
        ws.write(4, 0, fail_data[0], format_text)
        ws.write(4, 1, fail_data[1], format_top)
        ws.write(4, 2, fail_data[2], format_text)
        ws.write(4, 3, fail_data[3], format_text)

        # 创建新的sheet 静态失败SQL
        static_ws = wb.add_worksheet('静态失败SQL')
        static_ws.set_column(0, 1, 25)
        static_ws.set_column(1, 3, 70)

        static_fail_heads = parame_dict['static_fail_heads']
        static_fail_data = parame_dict['static_fail_data']
        [static_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(static_fail_heads)]
        for row_num, row in enumerate(static_fail_data):
            static_ws.write(row_num + 1, 0, row[0], format_text)
            static_ws.write(row_num + 1, 1, row[1], format_text)
            static_ws.write(row_num + 1, 2, row[2], format_text)

        # 创建新的sheet 静态失败SQL
        dynamic_ws = wb.add_worksheet('动态失败SQL')
        dynamic_ws.set_column(0, 1, 25)
        dynamic_ws.set_column(1, 3, 70)

        dynamic_fail_heads = parame_dict['dynamic_fail_heads']
        dynamic_fail_data = parame_dict['dynamic_fail_data']

        [dynamic_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(dynamic_fail_heads)]
        for row_num, row in enumerate(dynamic_fail_data):
            dynamic_ws.write(row_num + 1, 0, row[0], format_text)
            dynamic_ws.write(row_num + 1, 1, row[1], format_text)
            dynamic_ws.write(row_num + 1, 2, row[2], format_text)

        # 创建新的sheet 所有检测SQL
        all_ws = wb.add_worksheet('所有检测SQL')
        all_ws.set_column(0, 1, 25)
        all_ws.set_column(1, 4, 70)
        all_work_heads = parame_dict['all_work_heads']
        all_work_data = parame_dict['all_work_data']

        [all_ws.write(0, x, field.upper(), format_title) for x, field in enumerate(all_work_heads)]
        for row_num, row in enumerate(all_work_data):
            all_ws.write(row_num + 1, 0, row[0], format_text)
            all_ws.write(row_num + 1, 1, row[1], format_text)
            all_ws.write(row_num + 1, 2, row[2], format_text)
            all_ws.write(row_num + 1, 3, row[3], format_text)

        wb.close()
        return path
