# Author: kk.Fang(fkfkbill@gmail.com)

import os
import gzip
import time
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
from past.rule_analysis.db.mongo_operat import MongoHelper

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
                     VALUES(SEQ_SENDMAIL_HIST.nextval, :1, :2, :3, :4, to_date(:5, 'yyyy-mm-dd hh24:mi:ss'))
                  """

    sql_params = [send_mail_id, login_user, file_path, result, get_time()]
    OracleHelper.insert(sql, params=sql_params)


@celery.task
def timing_send_email(send_user_list):
    """
    发送邮件
    :param send_user_list:
    :return:
    """
    sql = "SELECT * FROM MAIL_SERVER"
    server_data = OracleHelper.select_dict(sql, one=True)

    for send_detail in send_user_list:
        for login_user in send_detail['mail_sender']:
            params = [login_user]
            user_sql = "SELECT EMAIL FROM T_USER WHERE LOGIN_USER=:1"
            user_data = OracleHelper.select(user_sql, params=params, one=True)
            user_email = user_data[0]

            title = send_detail['title']
            contents = send_detail['contents']

            # 获取EXCEL打包后的压缩包
            path = create_excel(login_user, send_detail['send_mail_id'])
            result, error = send_mail(title, contents, user_email, server_data, path,
                                      settings.CLIENT_NAME + 'SQL审核报告' + datetime.now().strftime("%Y_%m_%d") + '.zip')
            result = 1 if result else 0
            error = str(error) if error is not None else ''
            update_send_mail(result, error, send_detail['send_mail_id'])
            insert_send_mail_history(login_user, send_detail['send_mail_id'], result, path)


def filter_data(data, filter_datetime=True):
    # 过滤拿出来的数据
    if data is None:
        return ""
    if isinstance(data, datetime) and filter_datetime:
        return data.strftime('%Y-%m-%d %X')
    return data


# 创建excel(SQL健康度数据、风险SQL)


def create_excel(username, send_list_id):
    path = "/tmp/" + username + str(int(time.time()))
    if not os.path.exists(path):
        os.makedirs(path)

    cmdb_ids = get_cmdb_ids()

    if username != "admin":

        query = """SELECT cmdb_id, schema_name FROM T_DATA_PRIVILEGE WHERE login_user = :1"""
        res = OracleHelper.select_dict(query, [username], one=False)

        data = {}
        for value in res:
            if value['cmdb_id'] in data:
                data[value['cmdb_id']].append(value['schema_name'])
            else:
                data[value['cmdb_id']] = [value['schema_name']]

        for cmdb_id, value in data.items():
            if cmdb_id not in cmdb_ids:
                continue
            query = """select connect_name from T_CMDB where cmdb_id = :1"""
            connect_name = OracleHelper.select(query, [cmdb_id], one=True)
            wb = xlsxwriter.Workbook(
                path + "/" + connect_name[0] + datetime.now().strftime("%Y%m%d") + ".xlsx")
            create_sql_healthy_file(cmdb_id, value, username, wb)
            create_risk_obj_file(cmdb_id, value, username, wb)
            create_risk_sql_file(cmdb_id, value, username, wb)
            create_appendx(wb)
            wb.close()
    else:
        for cmdb_id in cmdb_ids:
            query = """select connect_name from T_CMDB where cmdb_id = :1"""
            connect_name = OracleHelper.select(query, [cmdb_id], one=True)
            wb = xlsxwriter.Workbook(
                path + "/" + connect_name[0] + datetime.now().strftime("%Y%m%d") + ".xlsx")
            create_sql_healthy_file(cmdb_id, [], username, wb)
            create_risk_obj_file(cmdb_id, [], username, wb)
            create_risk_sql_file(cmdb_id, [], username, wb)
            create_appendx(wb)
            wb.close()

    file_path_list = [
        settings.CLIENT_NAME,
        str(send_list_id),
        datetime.now().strftime("%Y%m%d%H%M") + ".zip"
    ]
    # print(ROOT_PATH + "/webui/static/files/mail_files/", ''.join(file_path_list))
    zipPath = zip_file_path(
        path, ROOT_PATH + "/webui/static/files/mail_files/", ''.join(file_path_list))
    return zipPath


# 创建sql健康度EXCEL
def create_sql_healthy_file(cmdb_id, schemas, login_user, wb):
    ws = wb.add_worksheet("1.整体健康度")

    query = """select connect_name from T_CMDB where cmdb_id = :1"""
    connect_name = OracleHelper.select(query, [cmdb_id], one=True)

    if not connect_name:
        return
    # excel 表格样式sheet1
    ws.set_column(0, 7, 18)
    head_merge_format = wb.add_format({
        'size': 18,
        'bold': 1,
        'align': 'center',
        'valign': 'vcenter',
    })
    ws.set_row(0, 30, head_merge_format)
    ws.merge_range('A1:H1', settings.CLIENT_NAME +
                   "“" + connect_name[0] + "”SQL风险评估报告")
    # ws.write_rich_string('A1', red, head_merge_format)
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
    ws.write(1, 7, datetime.now().strftime("%Y/%m/%d"), text_format)
    ws.set_row(3, 20)
    ws.merge_range('A4:H4', "1.最近7天整体健康度", title_format)
    content_format = wb.add_format({
        'color': 'red',
        'align': 'left',
        'valign': 'vcenter',

    })
    ws.write(4, 0, "采集日期", title_format)
    ws.write(5, 0, "得分")
    start_time = datetime.now() - timedelta(days=7)
    query = """select d.* from T_CMDB c join T_DATA_HEALTH  d on c.connect_name = d.database_name
             where c.cmdb_id = :1  and d.collect_date >= :2 order by d.collect_date"""
    res = OracleHelper.select(query, params=[cmdb_id, start_time], one=False)
    # 填充表格健康度数据
    # ----------------start---------------------
    col = 1
    if res:
        for value in res:
            ws.write(4, col, value[2].strftime("%Y-%m-%d"), date_format)
            if value[1] < 75:
                ws.write(5, col, value[1], content_format)
            else:
                ws.write(5, col, value[1], text_format)
            col += 1
    # --------------------------end-------------------
    # 根据健康度表格数据绘制折线图
    # --------------------------------start--------------------
    chart1 = wb.add_chart({'type': 'line'})
    chart1.width = 650
    chart1.height = 350
    # Configure the first series.
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
        },
    })

    # 设置折线图的位置，标题，x轴，y轴
    # --------------------start-------------------------
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
    # --------------------------------end-----------------------------
    # -------------------------------------end------------------------
    sql = {"create_time": {"$gte": start_time.strftime("%Y-%m-%d %X")}}

    if login_user != "admin":
        sql.update({'cmdb_id': cmdb_id, 'desc.owner': {"$in": schemas}})
    else:
        sql.update({'cmdb_id': cmdb_id})

    records = MongoHelper.find("job", sql)
    records = sorted(records, key=lambda x: (
        x['create_time'], x['name']), reverse=True)
    status_map = {
        "0": "失败",
        "1": "成功",
        "2": "正在运行"
    }
    # 健康度下钻表格样式设置及填充
    # --------------------------start-----------------------------
    ws.set_row(27, 20)
    ws.merge_range('A27:H27', "2.健康度下钻", title_format)
    ws.set_row(28, 20)
    titles = ['审计目标', '审计用户', '创建时间', '状态', '类型', '分数', '开始时间', '结束时间']
    ws.write_row('A28', titles, titles_format)

    row = 28
    col = 0
    for value in records:
        ws.write(row, col, value['connect_name'], text_format)
        ws.write(row, col + 1, value["name"].split("#")[0], text_format)
        ws.write(row, col + 2, value["create_time"][:-3], text_format)
        ws.write(row, col + 3, status_map[value['status']], text_format)
        ws.write(row, col + 4, value["name"].split("#")[1], text_format)
        if int(value.get("score", 0)) < 75:
            ws.write(row, col + 5, value.get("score", ""), content_format)
        else:
            ws.write(row, col + 5, value.get("score", ""), text_format)
        ws.write(row, col + 6, value["desc"]
                               ["capture_time_s"][:-3], text_format)
        ws.write(row, col + 7, value["desc"]
                               ["capture_time_e"][:-3], text_format)
        row += 1


# ---------------------------------------------end---------------------------

# 创建风险SQL EXCELcmdb_id
def create_risk_sql_file(cmdb_id, schemas, login_user, wb):
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

    task_info = MongoHelper.find('job', condition, {"id": 1, "_id": 0})
    task_ids = [item['id'] for item in task_info]

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
                [set_dict[rule_name].add(
                    kvalue['sql_id']) for krisk, kvalue in sql_ids.items() if krisk != 'scores']
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
    return target_filepath


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

    sql = "SELECT RULE_NAME, RISK_NAME, RISK_SQL_DIMENSION, SEVERITY, OPTIMIZED_ADVICE FROM T_RISK_SQL_RULE"
    data = OracleHelper.select(sql, one=False)

    for index, row in enumerate(data):
        ws.write_row(index + 1, 0, row, content_format)


# 风险对象


def create_risk_obj_file(cmdb_id, owner_list, login_user, wb):
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
    timing_send_email.delay([{'title': "测试", 'contents': "测试发邮件",
                              'mail_sender': ['operator'], 'send_mail_id': 1001}])