# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from collections import defaultdict

import plain_db.oracleob
import past.rule_analysis.db.mongo_operat
import past.utils.utils
from utils.datetime_utils import *

'''
获取mongodb内数据库级别的统计数据
'''


def save_scores(job, score):
    sql = """
            INSERT INTO T_DATA_HEALTH_USER(database_name, username, type, health_score, collect_time, collect_date)
            VALUES(:1, :2, :3, :4, :5, :6)
          """
    now = datetime.now()
    today = now - timedelta(hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    plain_db.oracleob.OracleHelper.insert(sql, [job['desc']['instance_name'], job['desc']['owner'],
                                                job['desc']['rule_type'].lower(), score, now, today])


def calculate(task_exec_hist_id):
    cmdbs = plain_db.oracleob.OracleHelper.select("SELECT cmdb_id, connect_name, service_name FROM T_CMDB", one=False)
    cmdb_id2name = {x[0]: x[1] for x in cmdbs}

    sql = "SELECT database_name, username FROM T_DATA_HEALTH_USER_CONFIG WHERE needcalc = 'Y'"
    weights = defaultdict(list)
    for row in plain_db.oracleob.OracleHelper.select_dict(sql, one=False):
        weights[row['database_name']].append(row['username'])

    scores = defaultdict(lambda _=None: defaultdict(list))

    # today = datetime.now()
    # tomorrow = today + timedelta(days=1)
    # sql = {'create_time': {'$gte': today.date(), '$lt': tomorrow.date()}}
    sql = {"record_id": {"$regex": f"^{task_exec_hist_id}"}}
    for job in past.rule_analysis.db.mongo_operat.MongoHelper.find("job", sql):

        if job['desc']['owner'] not in weights[job['connect_name']]:
            # print(f" *** schema({job['desc']['owner']}) not in {weights[job['connect_name']]}")
            continue

        date = d_to_str(job['create_time'].date())
        select = {'_id': 0, 'task_uuid': 0, 'username': 0, 'ip_address': 0, 'sid': 0, 'create_time': 0}
        result = past.rule_analysis.db.mongo_operat.MongoHelper.find_one("results", {'task_uuid': str(job['_id'])},
                                                                         select)

        if not result:
            continue

        # mongo里有数据但是oracle里没有
        if result['cmdb_id'] not in cmdb_id2name:
            continue

        connect_name = cmdb_id2name[result['cmdb_id']]
        instance_name = connect_name

        search_temp = {"DB_IP": job["desc"]["db_ip"], "OWNER": job["desc"]["owner"],
                       'INSTANCE_NAME': job["desc"].get("instance_name", "空")}
        rules, score = past.utils.utils.calculate_rules(result, search_temp)

        past.rule_analysis.db.mongo_operat.MongoHelper.update_one('job', {'_id': job['_id']},
                                                                  {"$set": {'score': score}})
        save_scores(job, score)

    # ====================================================================================

    for dbname, data in scores.items():
        for day, scores in data.items():
            score = sum(scores) // len(scores)
            sql = """SELECT database_name
                     FROM T_DATA_HEALTH
                     WHERE database_name = :1 AND collect_date = to_date(:2, 'yyyy-mm-dd')
                  """
            exist = plain_db.oracleob.OracleHelper.select(sql, [dbname, day], one=True)
            if not exist:
                sql = "INSERT INTO T_DATA_HEALTH(database_name, collect_date, health_score, id) VALUES(:1, to_date(:2, 'yyyy-mm-dd'), :3, SEQ_DATA_HEALTH.nextval)"
                plain_db.oracleob.OracleHelper.insert(sql, [dbname, day, score])
            else:
                sql = "UPDATE T_DATA_HEALTH set health_score=:1 WHERE database_name = :2 AND collect_date = to_date(:3, 'yyyy-mm-dd')"
                plain_db.oracleob.OracleHelper.update(sql, [score, dbname, day])

# if __name__ == "__main__":
#     calculate()
