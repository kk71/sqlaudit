# Author: kk.Fang(fkfkbill@gmail.com)

import re
import os
import gzip
import time
import arrow
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta

import settings
import xlsxwriter
from past.utils.send_mail import send_mail
from plain_db.oracleob import OracleHelper
from past.utils.utils import get_risk_obj_body
from past.models import get_rules as get_rules
from past.models import get_cmdb
from past.utils.utils import get_time
from past.utils.utils import ROOT_PATH
import utils.cmdb_utils
from models.oracle import make_session, CMDB
from models.mongo import Results, StatsCMDBRate
from past.rule_analysis.db.mongo_operat import MongoHelper
from past.utils.send_mail import send_work_list_status
from utils import cmdb_utils
from utils.sql_utils import risk_sql_export_data
from utils.object_utils import risk_object_export_data

from .base import celery


def update_send_mail(result, error, send_mail_id):
    last_send_date = datetime.now()
    sql = """
            UPDATE SEND_MAIL_LIST
            SET last_send_status=:1, error_msg=:2, last_send_date=:3
            WHERE send_mail_id= :4
          """
    sql_params = [result, str(error), last_send_date, send_mail_id]
    OracleHelper.update(sql, params=sql_params)


def insert_send_mail_history(login_user, send_mail_id, result, file_path):
    sql = """INSERT INTO t_send_mail_hist(id, send_mail_list_id, receiver, file_path, status, create_time)
                     VALUES(SEQ_SENDMAIL_HIST.nextval, :1, :2, :3, :4, :5)
                  """

    sql_params = [send_mail_id, login_user, file_path, result, get_time()]
    OracleHelper.insert(sql, params=sql_params)


@celery.task
def timing_send_work_list_status(work_list):
    """
    发送线下审核工单状态
    :param work_list:
    :return:
    """
    sql = "SELECT * FROM MAIL_SERVER"
    server_data = OracleHelper.select_dict(sql, one=True)

    params = [work_list.submit_owner]
    user_sql = "SELECT EMAIL FROM T_USER WHERE LOGIN_USER=:1"
    user_data = OracleHelper.select(user_sql, params=params, one=True)
    user_email = user_data[0]

    work_list_status = work_list.work_list_status
    audit_date = work_list.audit_date
    audit_comments = work_list.audit_comments
    audit_owner = work_list.audit_owner
    work_list_id = work_list.work_list_id
    task_name = work_list.task_name
    comment = None
    if work_list_status == 1:
        comment = f"工单id为{work_list_id},任务名称为{task_name}的工单,在{audit_date}被{audit_owner}审核通过"
    elif work_list_status == 2:
        comment = f"工单id为{work_list_id},任务名称为{task_name}的工单,在{audit_date}被{audit_owner}审核不通过,理由为{audit_comments}"

    send_work_list_status(server_data, user_email, comment)


@celery.task
def timing_send_mail(send_user_list):
    """
    发送邮件
    :param send_user_list:
    :return:
    """
    sql = "SELECT * FROM MAIL_SERVER"
    server_data = OracleHelper.select_dict(sql, one=True)

    for send_detail in send_user_list:
        for login_user in send_detail['user_type_name_list']:
            params = [login_user]
            user_sql = "SELECT EMAIL FROM T_USER WHERE LOGIN_USER=:1"
            user_data = OracleHelper.select(user_sql, params=params, one=True)
            user_email = user_data[0]

            title = send_detail['title']
            contents = send_detail['contents']

            # 获取EXCEL打包后的压缩包
            path = create_excels(login_user, send_detail['send_mail_id'])
            result, error = send_mail(title, contents, user_email, server_data, path,
                                      settings.CLIENT_NAME + 'SQL审核报告' + datetime.now().strftime("%Y_%m_%d") + '.zip')
            result = 1 if result else 0
            error = str(error) if error is not None else ''
            update_send_mail(result, error, send_detail['send_mail_id'])
            insert_send_mail_history(login_user, send_detail['send_mail_id'], result, os.path.join("/", path))


def filter_data(data, filter_datetime=True):
    # 过滤拿出来的数据
    if data is None:
        return ""
    if isinstance(data, datetime) and filter_datetime:
        return data.strftime('%Y-%m-%d %X')
    return data


# 创建excel(SQL健康度数据、风险SQL,风险对象)
def create_excels(username, send_list_id):
    """new create excels """
    path = "/tmp/" + username + str(int(time.time()))
    if not os.path.exists(path):
        os.makedirs(path)

    with make_session() as session:
        cmdb_ids: list = cmdb_utils.get_current_cmdb(session, username)

        date_start = arrow.get(str(arrow.now().date()), 'YYYY-MM-DD').shift(days=-6).date()
        date_end = arrow.get(str(arrow.now().date()), 'YYYY-MM-DD').shift(days=+1).date()
        date_start_today = arrow.get(str(arrow.now().date()), 'YYYY-MM-DD').date()
        now = arrow.now()

        if cmdb_ids == []:
            wb = xlsxwriter.Workbook(
                path + "/" + "此用户无纳管库" + "-" + arrow.now().date().strftime("%Y%m%d") + ".xlsx")
            wb.close()
        for cmdb_id in cmdb_ids:
            connect_name = session.query(CMDB.connect_name).filter_by(cmdb_id=cmdb_id)[0][0]
            cmdb_rate_q = StatsCMDBRate.objects().filter(connect_name=connect_name,
                                                         etl_date__gt=now.shift(weeks=-1).datetime). \
                order_by("etl_date")
            cmdb_rate = [x.to_dict() for x in cmdb_rate_q]

            rst_q = Results.objects(cmdb_id=cmdb_id, score__score__nin=[None, 0],
                                    create_date__gte=date_start,
                                    create_date__lte=date_end).order_by("-create_date")
            rst_d = [x.to_dict() for x in rst_q]

            rr_obj, rst_obj = risk_object_export_data(
                cmdb_id=cmdb_id, date_start=date_start_today,
                date_end=date_end)

            rr_sql, rst_sql = risk_sql_export_data(
                cmdb_id=cmdb_id,
                date_start=date_start_today,
                date_end=date_end)

            wb = xlsxwriter.Workbook(
                path + "/" + connect_name + "-" + arrow.now().date().strftime("%Y%m%d") + ".xlsx")
            create_sql_healthy_files(rst_d, cmdb_rate, connect_name, wb)
            create_risk_obj_files(rr_obj, rst_obj, wb)
            create_risk_sql_files(rr_sql, rst_sql, wb)
            wb.close()

    file_path_list = [
        settings.CLIENT_NAME,
        str(send_list_id),
        datetime.now().strftime("%Y%m%d%H%M") + ".zip"
    ]
    zipPath = zip_file_path(
        path, ROOT_PATH + "/downloads/mail_files/", ''.join(file_path_list))
    ret_url = os.path.join("downloads/mail_files/", ''.join(file_path_list))
    return ret_url


# 创建sql健康度EXCEL
def create_sql_healthy_files(rst_d, cmdb_rate, connect_name, wb):
    # excel 表格样式sheet1
    ws = wb.add_worksheet("1.整体健康度")
    ws.set_column(0, 7, 18)
    head_merge_format = wb.add_format({
        'size': 18,
        'bold': 1,
        'align': 'center',
        'valign': 'vcenter',
    })
    ws.set_row(0, 30, head_merge_format)
    ws.merge_range('A1:H1', settings.CLIENT_NAME +
                   "“" + connect_name + "”SQL风险评估报告")

    # 1.最近7天的整体健康度
    title_format = wb.add_format({
        "size": 14,
        'bold': 1,
        'align': 'left',
        'valign': 'vcenter',
    })
    titles_format = wb.add_format({
        "size": 14,
        'bold': 1,
        'align': 'center',
        'valign': 'vcenter',
    })
    text_format = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
    })
    date_format = wb.add_format({
        'bold': 1,
        'align': 'left',
        'valign': 'vcenter',
    })
    ws.write(1, 6, "报告日期", text_format)
    ws.write(1, 7, arrow.now().date().strftime("%Y/%m/%d"), text_format)
    ws.set_row(3, 20)
    ws.merge_range('A4:H4', "1.最近7天整体健康度", title_format)
    ws.write(4, 0, "采集日期", title_format)
    ws.write(5, 0, "得分")
    col = 1
    for cr in cmdb_rate:
        ws.write(4, col, cr['etl_date'][:10], date_format)
        ws.write(5, col, cr['score'], text_format)
        col += 1

    # 根据健康度表格数据绘制折线图
    chart1 = wb.add_chart({'type': 'line'})
    chart1.width = 650
    chart1.height = 350

    chart1.add_series({
        'name': '健康度',
        'categories': ['1.整体健康度', 4, 1, 4, 7],
        'values': ['1.整体健康度', 5, 1, 5, 7],
        'line': {'color': 'black', 'width': 1},
        'marker': {'type': 'automatic',
                   },
        'data_labels': {'value': True},
        'name_font': {
            'size': 10,
        }
    })
    # 设置折线图的位置，标题，x轴，y轴
    chart1.set_title({'name': '最近7天整体健康度',
                      'name_font': {
                          'bold': False,
                          'size': 14, },
                      })
    chart1.set_plotarea({
        'layout': {
            'x': 0.1,
            'y': 0.2,
            'width': 0.88,
            'height': 0.63,
        }
    })
    chart1.set_legend({
        'layout': {
            'x': 0.90,
            'y': 0.05,
            'width': 0.2,
            'height': 0.1,
        }
    })
    chart1.set_y_axis({'name': '健康度分数',
                       'min': 0,
                       'max': 100,
                       'name_font': {
                           'bold': False,
                           'size': 12,
                       }
                       })
    ws.insert_chart('B7', chart1, {'x_offset': 25, 'y_offset': 20})

    # -----------------------------------------
    # 健康度下钻表格样式设置及填充
    ws.set_row(27, 20)
    ws.merge_range('A27:H27', "2.健康度下钻", title_format)
    ws.set_row(28, 20)
    titles = ['审计目标', '审计用户', '创建时间', '类型', '分数', '开始时间', '结束时间']
    ws.write_row('A28', titles, titles_format)

    row = 28
    col = 0
    for rst in rst_d:
        ws.write(row, col, rst["score"]["connect_name"], text_format)
        ws.write(row, col + 1, rst["score"]["schema_name"], text_format)
        ws.write(row, col + 2, rst["create_date"], text_format)
        ws.write(row, col + 4, rst["score"]["rule_type"], text_format)
        ws.write(row, col + 5, rst["score"]["score"], text_format)
        ws.write(row, col + 6, str(rst["capture_time_start"]), text_format)
        ws.write(row, col + 7, str(rst["capture_time_end"]), text_format)
        row += 1


# 创建风险SQL EXCEL
def create_risk_sql_files(risk_sql_outer, risk_sql_inner, wb):
    """new create risk sql files"""
    outer_title_heads = ["采集时间", "schema名称", "风险分类名称", "风险等级", "扫描得到合计", "一次采集id"]
    inner_heads = ["SQL ID", 'SQL_TEXT']

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
    for row_num, r_s_outer in enumerate(risk_sql_outer):
        a += 1
        row_num = 0
        ws = wb.add_worksheet(re.sub('[*%]', '', r_s_outer['rule_desc'][:20]) + f'-{a}')
        ws.set_row(0, 20, title_format)
        ws.set_column(0, 0, 60)
        ws.set_column(1, 1, 60)
        ws.set_column(2, 2, 60)

        [ws.write(0, x, field, title_format) for x, field in enumerate(outer_title_heads)]
        row_num += 1
        ws.write(row_num, 0, r_s_outer["etl_date"], content_format)
        ws.write(row_num, 1, r_s_outer["schema"], content_format)
        ws.write(row_num, 2, r_s_outer["rule_desc"], content_format)
        ws.write(row_num, 3, r_s_outer["severity"], content_format)
        ws.write(row_num, 4, r_s_outer["rule_num"], content_format)
        ws.write(row_num, 5, r_s_outer["task_record_id"], content_format)

        rows_nums = 1
        for r_s_inner in risk_sql_inner:
            [ws.write(3, x, field, title_format) for x, field in enumerate(inner_heads)]
            if r_s_inner['task_record_id'] in list(r_s_outer.values()) and \
                    r_s_inner['schema'] in list(r_s_outer.values()) and \
                    r_s_inner['rule']['rule_name'] in list(r_s_outer.values()):
                ws.write(3 + rows_nums, 0, r_s_inner['sql_id'], content_format)
                ws.write(3 + rows_nums, 1, r_s_inner['sql_text'], content_format)
                rows_nums += 1


def create_risk_sql_file(cmdb_id, schemas, login_user, wb):
    # TODO DEPRECATED
    """old create risk sql files"""
    ws = wb.add_worksheet("3.风险sql")

    start_time = datetime.now() - timedelta(days=14)
    if not cmdb_id:
        return {'code': 1, 'msg': '请选择一个cmdb'}

    cmdb_id = int(cmdb_id)
    query = """select * from T_CMDB where cmdb_id = :1"""
    cmdb = OracleHelper.select_dict(query, [cmdb_id], one=True)
    if not cmdb:
        return

    # whitelist_dict = construct_whitelist(cmdb_id)

    # 第一步：查找到job 找到对应的id
    condition = {
        "cmdb_id": cmdb_id,
        # "desc.owner": {"$in": schemas},
        "create_time": {"$gte": start_time.strftime("%Y-%m-%d")}
    }

    if login_user != "admin":
        condition["desc.owner"] = {"$in": schemas}

    task_info = MongoHelper.find('job', condition, {"_id": 1})
    task_ids = [str(item['_id']) for item in task_info]

    rule_list = get_rules("both")
    rule_list = [x for rules in rule_list for x in rules]

    # 获得风险分类
    risk_rules = get_risk_rules(
        select=['RULE_NAME', 'RISK_NAME', 'RISK_SQL_RULE_ID', 'OPTIMIZED_ADVICE'])
    rule_dict = {row['rule_name']: row['risk_name'] for row in risk_rules}
    # rule_id_dict = {row['rule_name']: row['risk_sql_rule_id'] for row in risk_rules}
    risk2advice = {row['rule_name']: row['optimized_advice']
                   for row in risk_rules}

    results = MongoHelper.find("results", {"task_uuid": {"$in": task_ids}}, {
        "_id": 0, "task_uuid": 0})
    risk_sql_dict = format_results(results, rule_list)

    risk_sql_list = [x for values in risk_sql_dict.values() for x in values]
    sql_text_info = []

    for part in range(len(risk_sql_list) // 200 + 1):
        sql_ids = risk_sql_list[200 * part: 200 * (part + 1)]

        condition = {
            'IPADDR': cmdb['ip_address'],
            'DB_SID': cmdb['service_name'],
            'SQL_ID': {'$in': sql_ids},
            'ETL_DATE': {"$gte": start_time.strftime("%Y-%m-%d %X")}
        }

        sql = [
            {"$match": condition},
            {"$group": {
                "_id": "$SQL_ID",
                "first_appear": {"$min": "$ETL_DATE"},
                "record_id": {"$max": "$record_id"},
                "SQL_TEXT": {"$max": "$SQL_TEXT"},
                "SQL_TEXT_DETAIL": {"$max": "$SQL_TEXT_DETAIL"},
                "USERNAME": {"$max": "$USERNAME"},
                "last_appear": {"$max": "$ETL_DATE"},
            }}
        ]

        sql_text_info.extend([x for x in MongoHelper.aggregate("sqltext", sql)])

    sql_text_info = {text_detail['_id']: text_detail for text_detail in sql_text_info}
    sql_stat_info = {}
    for text_key, text_value in sql_text_info.items():
        record_id = text_value["record_id"]
        sql_id = text_value['_id']
        select = {"EXECUTIONS_DELTA": 1, "ELAPSED_TIME_DELTA": 1, '_id': 0}
        etl_date = MongoHelper.find("sqlstat", {'SQL_ID': sql_id, 'record_id': record_id}, select,
                                    sort=[("ETL_DATE", -1)], limit=1)
        sql_stat_info.update({text_key: etl_date_detail for etl_date_detail in etl_date})

    # 去除重复的
    sqlid_user = []
    risk_sql_result = []
    for rule_name, sql_ids in risk_sql_dict.items():
        for sql_id in sql_ids:
            sqliduser = sql_id + '@' + rule_name
            if sql_id in sql_text_info and sqliduser not in sqlid_user:
                text_detail = sql_text_info[sql_id]
                # 如果在白名单里 就放过
                # if check_whitelist(whitelist_dict, text_detail):
                #     continue
                sqlid_user.append(sqliduser)
                item_name = rule_dict.get(rule_name, '')
                solution = risk2advice[rule_name]
                first = text_detail['first_appear']
                last = text_detail['last_appear']

                sql_stat_detail = sql_stat_info[sql_id]
                executions_delta = sql_stat_detail.get('EXECUTIONS_DELTA', 0)
                elapsed_time_delta = sql_stat_detail.get('ELAPSED_TIME_DELTA', 0)
                executions_eff = round(elapsed_time_delta / executions_delta, 2) if executions_delta else 0

                risk_sql_result.append(
                    [sql_id, item_name, solution, first[:-3], last[:-3], 1, text_detail['SQL_TEXT_DETAIL'],
                     executions_delta, elapsed_time_delta, executions_eff])
    risk_sql_result = sorted(risk_sql_result, key=lambda row: float(row[9]), reverse=True)

    ws.set_column(0, 0, 15)
    ws.set_column(1, 1, 20)
    ws.set_column(2, 2, 25)
    ws.set_column(3, 4, 16)
    ws.set_column(5, 5, 10)
    ws.set_column(6, 6, 40)
    ws.set_column(7, 7, 40)
    ws.set_column(8, 8, 40)
    ws.set_column(9, 9, 40)
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
    ws.write(0, 0, "SQL_ID", format_title)
    ws.write(0, 1, "风险点", format_title)
    ws.write(0, 2, "优化建议", format_title)
    ws.write(0, 3, "开始出现时间", format_title)
    ws.write(0, 4, "最近执行", format_title)
    ws.write(0, 5, "相似SQL", format_title)
    ws.write(0, 6, "SQL文本", format_title)
    ws.write(0, 7, "上次执行次数", format_title)
    ws.write(0, 8, "上次执行总时间(ms)", format_title)
    ws.write(0, 9, "上次平均时间", format_title)
    row = 1
    for value in risk_sql_result:
        ws.write_row(row, 0, value, format_text)
        row += 1


def format_results(results, rule_list, key='rule_name') -> dict:
    set_dict = defaultdict(set)
    for result in results:
        for rule_name, sql_ids in result.items():
            if rule_name not in rule_list or not sql_ids or 'records' in sql_ids:
                continue
            if key == 'rule_name':
                kvalues = [kvalue for krisk, kvalue in sql_ids.items() if krisk != 'scores']
                [set_dict[rule_name].add(
                    kvalue['sql_id']) for k in kvalues for kvalue in k]
            elif key == 'schemas':
                [set_dict[result['username']].add(kvalue['sql_id']) for krisk, kvalue in sql_ids.items() if
                 krisk != 'scores']

    return set_dict


def check_whitelist(whitelist_dict, text_detail):
    # 3. check sqltext
    # 2. check user
    # 1. check modal
    if text_detail['USERNAME'] in whitelist_dict['user']:
        return True

    for sqltext in whitelist_dict['sqltext']:
        if sqltext in text_detail['SQL_TEXT']:
            return True

    return False


def construct_whitelist(cmdb_id):
    whitelist_dict = {'user': [], 'modal': [], 'sqltext': []}
    rule_catagory = {1: 'user', 2: 'modal', 3: 'sqltext'}
    sql = "SELECT * FROM WHITE_LIST_RULES WHERE cmdb_id = :1"
    whitelist = OracleHelper.select_dict(sql, [cmdb_id], one=False)
    for row in whitelist:
        if row['status'] == 1:
            whitelist_dict[rule_catagory[row['rule_catagory']]].append(
                row['rule_text'])

    return whitelist_dict


def get_risk_rules(select=["*"]):
    sql = f"SELECT {', '.join(select)} FROM T_RISK_SQL_RULE"
    return OracleHelper.select_dict(sql, one=False)


def gzip_file(before_filepath, gzip_filepath):
    for _, _, files in os.walk(before_filepath):
        for file_name in files:
            gzip_fd = gzip.open(os.path.join(gzip_filepath, file_name), 'wb')
            filename = os.path.join(before_filepath, file_name)
            with open(filename, 'rb') as f:
                content = f.read()
            gzip_fd.write(content)
            gzip_fd.close()
    return ""


def get_zip_file(input_path, result):
    files = os.listdir(input_path)
    for file in files:
        if os.path.isdir(input_path + '/' + file):
            get_zip_file(input_path + '/' + file, result)
        else:
            result.append(input_path + '/' + file)


def zip_file_path(input_path, output_path, output_name):
    target_filepath = os.path.join(output_path, output_name)
    f = zipfile.ZipFile(target_filepath, 'w', zipfile.ZIP_DEFLATED)
    filelists = []
    get_zip_file(input_path, filelists)
    for dirpath, dirnames, filenames in os.walk(input_path):
        for filename in filenames:
            f.write(os.path.join(dirpath, filename), filename)

    f.close()  # 调用了close方法才会保证完成压缩
    ret_url = os.path.join(settings.EXPORT_PREFIX_HEALTH, output_name)
    return ret_url


def get_cmdb_ids():
    sql = "SELECT cmdb_id FROM T_CMDB"
    return [x[0] for x in OracleHelper.select(sql, one=False)]


def create_appendx(wb):
    # 获取附录文件内容
    ws = wb.add_worksheet("4.附录<<风险规则管理>>")
    title = ["规则名称", "风险名称", "风险类型", "风险等级", "优化建议"]
    title_format = wb.add_format({
        'size': 14,
        'bold': 1,
        'align': 'center',
        'valign': 'vcenter',
    })
    content_format = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
    })
    ws.set_column(0, 0, 30)
    ws.set_column(1, 1, 30)
    ws.set_column(2, 3, 15)
    ws.set_column(4, 4, 55)
    ws.write_row(0, 0, title, title_format)

    sql = "SELECT RULE_NAME, RISK_NAME, RULE_TYPE, SEVERITY, OPTIMIZED_ADVICE FROM T_RISK_SQL_RULE"
    data = OracleHelper.select(sql, one=False)

    for index, row in enumerate(data):
        ws.write_row(index + 1, 0, row, content_format)


# 风险对象
def create_risk_obj_files(risk_obj_outer, risk_obj_inner, wb):
    outer_title_heads = ["采集时间","schema名称","风险分类名称", "风险等级","扫描得到合计","影响","优化建议","一次采集id"]
    inner_heads = ['对象名称', '对象类型', '风险问题']

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
    for row_num, r_o_outer in enumerate(risk_obj_outer):
        a += 1
        row_num = 0
        ws = wb.add_worksheet(re.sub('[*%]', '', r_o_outer["rule_desc"][:20]) + f'-{a}')
        ws.set_row(0, 20, title_format)
        ws.set_column(0, 0, 60)
        ws.set_column(1, 1, 60)
        ws.set_column(2, 2, 60)

        [ws.write(0, x, field, title_format) for x, field in enumerate(outer_title_heads)]
        row_num += 1
        ws.write(row_num, 0, r_o_outer["etl_date"], content_format)
        ws.write(row_num, 1, r_o_outer["schema"], content_format)
        ws.write(row_num, 2, r_o_outer["rule_desc"], content_format)
        ws.write(row_num, 3, r_o_outer["severity"], content_format)
        ws.write(row_num, 4, r_o_outer["rule_num"], content_format)
        ws.write(row_num, 5, r_o_outer["influence"], content_format)
        ws.write(row_num, 6, r_o_outer["optimized_advice"], content_format)
        ws.write(row_num, 7, r_o_outer["task_record_id"], content_format)


        rows_nums = 1
        for r_o_inner in risk_obj_inner:
            [ws.write(3, x, field, title_format) for x, field in enumerate(inner_heads)]
            if r_o_inner['task_record_id'] in list(r_o_outer.values()) and \
                    r_o_inner['schema'] in list(r_o_outer.values()) and \
                    r_o_inner['rule']['rule_name'] in list(r_o_outer.values()):

                ws.write(3 + rows_nums, 0, r_o_inner['object_name'], content_format)
                ws.write(3 + rows_nums, 1, r_o_inner['rule']['obj_info_type'], content_format)
                ws.write(3 + rows_nums, 2, r_o_inner['risk_detail'], content_format)
                rows_nums += 1


def create_risk_obj_file(cmdb_id, owner_list, login_user, wb):
    # TODO DEPRECATED
    ws = wb.add_worksheet("2.风险对象")
    title = ["对象名称", "风险点", "风险详情", "最早出现时间", "最后出现时间", "优化建议"]
    title_format = wb.add_format({
        'size': 14,
        'bold': 1,
        'align': 'center',
        'valign': 'vcenter',
    })
    content_format = wb.add_format({
        'align': 'left',
        'valign': 'vcenter',
        'text_wrap': True,
    })
    ws.set_row(0, 20, title_format)
    ws.set_column(0, 1, 20)
    ws.set_column(2, 2, 30)
    ws.set_column(3, 4, 16)
    ws.set_column(5, 5, 40)
    ws.write_row(0, 0, title)

    # whitelist_open = self.get_argument("whitelist_open", 1)  # 1 open. 0 close
    # whitelist_open = int(whitelist_open)

    cmdb_id = int(cmdb_id)
    db_model = get_cmdb(cmdb_id=cmdb_id)['db_model']
    obj_rules = get_rules("obj")
    now = datetime.now()
    query_start = (now - timedelta(days=7)).strftime("%Y-%m-%d %X")
    query_end = now.strftime("%Y-%m-%d %X")

    if login_user == "admin":
        owner_list = utils.cmdb_utils.get_cmdb_available_schemas(cmdb_id)

    risk_obj_result = get_risk_obj_body(
        cmdb_id, db_model, obj_rules, owner_list, query_start, query_end)
    risk_obj_result = [x[1:] for x in risk_obj_result]

    # 这个是数据
    for index, row in enumerate(risk_obj_result):
        ws.write_row(index + 1, 0, row[:3], content_format)
        ws.write(index + 1, 3, row[3][:-3], content_format)
        ws.write(index + 1, 4, row[4][:-3], content_format)
        ws.write(index + 1, 5, row[5], content_format)


if __name__ == "__main__":
    timing_send_mail.delay([{'title': "测试", 'contents': "测试发邮件",
                             'mail_sender': ['operator'], 'send_mail_id': 1001}])
