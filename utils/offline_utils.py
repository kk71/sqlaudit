# Author: kk.Fang(fkfkbill@gmail.com)

import re
import sqlparse
from redis import StrictRedis

import settings
from utils import const
from utils.datetime_utils import *
from utils import stream_utils
from utils.const import *


cache_redis_cli = StrictRedis(
    host=settings.CACHE_REDIS_IP,
    port=settings.CACHE_REDIS_PORT,
    db=settings.CACHE_REDIS_DB
)


def get_current_offline_ticket_task_name(submit_owner):
    """获取当前可用的线下审核任务名"""
    current_date = d_to_str(arrow.now().date(), fmt=COMMON_DATE_FORMAT_COMPACT)
    k = f"offline-ticket-task-num-{current_date}"
    current_num = "%03d" % cache_redis_cli.incr(k, 1)
    cache_redis_cli.expire(k, 60*60*24*3)  # 设置三天内超时
    return f"{submit_owner}-{current_date}-{current_num}"


def sql_processing(file_object,filter_sql_type) ->[dict]:
    """sql文件:编码,注释,sql;分割,类型获取过滤"""
    if file_object['body'].startswith(b"\xef\xbb\xbf"):
        body = file_object['body'][3:]
        encoding = 'utf-8'
    else:
        encoding = stream_utils.check_file_encoding(file_object['body'])
        body = file_object['body']
    try:
        body = body.decode(encoding)
    except UnicodeDecodeError:
        body = body.decode('utf-8')
    body = body.replace("\"", "'")

    # 下面这个处理是要把remark替换成普通单行注释，然后交给sqlparse去处理，
    # 处理完之后，再把remark加回去。
    REMARK_PLACEHOLDER: str = "--REMARKREMARK"
    tmpl_replaced_remark = re.compile(r"^\s*remark", re.I | re.M)
    sql_remark_replaced: str = tmpl_replaced_remark.sub(REMARK_PLACEHOLDER, body)
    sqls = []
    for sql in sqlparse.parse(sql_remark_replaced):
        if filter_sql_type is not None and \
                sql.get_type() in SQL_KEYWORDS[filter_sql_type]:
            # 判断是否需要过滤单句sql，并且判断当前这句sql是否在需要被过滤的列表里
            continue
        perfect_sql = sql.normalized.replace(REMARK_PLACEHOLDER, "remark").strip()
        if not perfect_sql:
            continue
        if perfect_sql[-1] == ";":
            perfect_sql = perfect_sql[:-1]

        # 分析当前导入的sql语句是什么类型的，ddl还是dml
        if sql.get_type() in SQL_KEYWORDS[SQL_DDL]:
            sql_type = SQL_DDL
        elif sql.get_type() in SQL_KEYWORDS[SQL_DML]:
            sql_type = SQL_DML
        else:
            sql_type = None

        # 以下返回结构应该与创建工单输入的sqls一致，方便前端对接
        sqls.append({
            "sql_text": perfect_sql,
            "sql_type": sql_type,
            "comments": ""
        })

    return sqls
