# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta
from collections import defaultdict

from webui.utils.oracleob import OracleHelper
from rule_analysis.db.mongo_operat import MongoHelper
from webui.utils.utils import calculate_rules

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
    OracleHelper.insert(sql, [job['desc']['instance_name'], job['desc']['owner'], job['desc']['rule_type'].lower(), score, now, today])

def calculate():

    cmdbs = OracleHelper.select("SELECT cmdb_id, connect_name, service_name FROM T_CMDB", one=False)
    cmdb_id2name = {x[0]: x[1] for x in cmdbs}

    sql = "SELECT database_name, username FROM T_DATA_HEALTH_USER_CONFIG WHERE needcalc = 'Y'"
    weights = defaultdict(list)
    for row in OracleHelper.select_dict(sql, one=False):
        weights[row['database_name']].append(row['username'])

    scores = defaultdict(lambda _=None: defaultdict(list))

    today = datetime.now()
    tomorrow = today + timedelta(days=1)
    sql = {'create_time': {'$gte': today.strftime('%Y-%m-%d'), '$lt': tomorrow.strftime('%Y-%m-%d')}}

    for job in MongoHelper.find("job", sql):

        if job['desc']['owner'] not in weights[job['desc']['instance_name']]:
            continue

        date = job['create_time'].split()[0]
        select = {'_id': 0, 'task_uuid': 0, 'username': 0, 'ip_address': 0, 'sid': 0, 'create_time': 0}
        result = MongoHelper.find_one("results", {'task_uuid': job['id']}, select)

        if not result:
            continue

        # mongo里有数据但是oracle里没有
        if result['cmdb_id'] not in cmdb_id2name:
            return

        connect_name = cmdb_id2name[result['cmdb_id']]
        instance_name = connect_name

        search_temp = {
            "DB_IP": job["desc"]["db_ip"],
            "OWNER": job["desc"]["owner"]
        }
        search_temp['INSTANCE_NAME'] = job["desc"].get("instance_name", "空")
        rules, score = calculate_rules(result, search_temp)

        if score < 100:
            scores[instance_name][date].append(score)

        MongoHelper.update_one('job', {'id': job['id']}, {"$set": {'score': score}})
        save_scores(job, score)

    # ====================================================================================

    for dbname, data in scores.items():
        for day, scores in data.items():
            score = sum(scores) // len(scores)
            sql = """SELECT database_name
                     FROM T_DATA_HEALTH
                     WHERE database_name = :1 AND collect_date = to_date(:2, 'yyyy-mm-dd')
                  """
            exist = OracleHelper.select(sql, [dbname, day], one=True)
            if not exist:
                sql = "INSERT INTO T_DATA_HEALTH(database_name, collect_date, health_score) VALUES(:1, to_date(:2, 'yyyy-mm-dd'), :3)"
                OracleHelper.insert(sql, [dbname, day, score])
            else:
                sql = "UPDATE T_DATA_HEALTH set health_score=:1 WHERE database_name = :2 AND collect_date = to_date(:3, 'yyyy-mm-dd')"
                OracleHelper.update(sql, [score, dbname, day])

if __name__ == "__main__":
    calculate()
