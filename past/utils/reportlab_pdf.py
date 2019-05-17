# -*- coding: utf-8 -*-

import os
from datetime import datetime
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.units import inch

import settings

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
file_name = 'msyh.ttf'
path = os.path.join(ROOT_PATH, 'script', file_name)

pdfmetrics.registerFont(TTFont('msyh', path))
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle


def create_pdf(component_data, login_user, user_name):
    story = []
    stylesheet = getSampleStyleSheet()
    normalStyle = stylesheet['Normal']

    curr_date = datetime.now().strftime("%Y-%m-%d")

    # 标题
    rpt_title = f'''<para autoLeading="off" fontSize=15 align=center><b><font face="msyh">
        {user_name} O18 SQL审核管控报告{curr_date}</font></b><br/><br/><br/></para>'''
    story.append(Paragraph(rpt_title, normalStyle))

    # text = '''<para autoLeading="off" fontSize=8><font face="msyh" >报告内容：</font><br/>
    # <font face="msyh" color=grey>1. 风险SQL列表 </font><br/>
    # <font face="msyh" color=grey>2. 系统主页详情 </font><br/>
    # <font face="msyh" color=grey>3. SQL健康度详情 </font><br/>
    # <font face="msyh" color=grey>4. 上线审核详情 </font><br/>
    # </para>'''
    # story.append(Paragraph(text, normalStyle))

    text = '<para autoLeading="off" fontSize=9><br/><br/><br/><b><font face="msyh">风险SQL列表：</font></b><br/></para>'
    story.append(Paragraph(text, normalStyle))

    # for component_list in component_data[1:]:
    #     sql_id = component_list[3]
    #     sql_href = f"<link href='{settings.SERVER_ADDRESS}/online_audit/sql_detail?sql_id={sql_id}' color='blue'>{sql_id}</link>"
    #     component_list[3] = Paragraph(sql_href, normalStyle)

    # # 表格数据：列表套列表
    # component_data = component_data

    # 创建表格对象，并设定各列宽度 不设置宽度会自动居中
    # component_table = Table(component_data, colWidths=[50, 50, 50, 50, 50, 50, 50, 50, 100])
    component_table = Table(component_data)

    # 添加表格样式
    component_table.setStyle(TableStyle([
        ('FONTNAME', (0, 0), (-1, -1), 'msyh'),  # 字体
        ('FONTSIZE', (0, 0), (-1, -1), 6),  # 字体大小
        ('ALIGN', (-1, 0), (-2, 0), 'RIGHT'),  # 对齐
        ('VALIGN', (-1, 0), (-2, 0), 'MIDDLE'),  # 对齐
        ('LINEBEFORE', (0, 0), (0, -1), 0.1, colors.grey),  # 设置表格左边线颜色为灰色，线宽为0.1
        ('TEXTCOLOR', (0, 1), (-2, -1), colors.black),  # 设置表格内文字颜色
        ('GRID', (0, 0), (-1, -1), 0.5, colors.black),  # 设置表格框线为红色，线宽为0.5
    ]))
    story.append(component_table)

    # text = '<para autoLeading="off" fontSize=9><br/><br/><br/><b><font face="msyh">系统主页详情：</font></b><br/></para>'
    # story.append(Paragraph(text, normalStyle))
    #
    # # 添加图片
    # img = Image('dashboard.png')
    # img.drawHeight = 350
    # img.drawWidth = 500
    #
    # component_data = [[img]]
    #
    # component_table = Table(component_data)
    # story.append(component_table)

    # 保存到指定路径
    # file_name = '风险SQL报告.pdf'
    # path = os.path.join(ROOT_PATH, 'webui/static', file_name)
    # 这里写死保存位置
    # path = '/tmp/merge.pdf'
    path = '/tmp/{}_merge.pdf'.format(login_user)
    print(path)
    doc = SimpleDocTemplate(path)
    doc.build(story)

    return path


if __name__ == '__main__':
    create_pdf([['1', '1', '1', '1']], 'admin')
    # cerate_result_pdf('admin', '123456')
