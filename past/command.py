import sys
import json
import datetime
import argparse

import settings

import past.rule_analysis.sqlaudit
import past.rule_analysis.db.db_operat
import past.rule_analysis.db.mongo_operat
import past.capture.sql_obj_info
import past.capture.sql_other_info
import past.utils.utils
import plain_db.oracleob

import utils.cmdb_utils
import utils.const
from utils.datetime_utils import *
import models.mongo

from pymongo.errors import DocumentTooLarge
from prettytable import PrettyTable


class Command(object):
    """
    一个简单的命令类，用来统一执行抓取模块，分析模块，导出模块，web管理模块
    """

    def __init__(self):
        self.mongo_client = past.rule_analysis.db.mongo_operat.MongoOperat(
            settings.MONGO_SERVER,
            settings.MONGO_PORT,
            settings.MONGO_DB,
            settings.MONGO_USER,
            settings.MONGO_PASSWORD
        )
        self.func = {
            "capture_obj": self.run_capture,
            "capture_other": self.run_capture,
            "analysis_o_obj": self.run_analysis,
            "analysis_o_plan": self.run_analysis,
            "analysis_o_stat": self.run_analysis,
            "analysis_o_text": self.run_analysis,
            "analysis_m_obj": self.run_analysis,
            "analysis_m_plan": self.run_analysis,
            "analysis_m_stat": self.run_analysis,
            "analysis_m_text": self.run_analysis,
        }

    def parse_init(self):
        self.parser = argparse.ArgumentParser("O18 sql review system...")
        self.parser.add_argument("-m", "--module", action="store", dest="module",
                                 help="指定模块类型，分为抓取模块，分析模块")
        self.parser.add_argument("-c", "--config", action="store", dest="config",
                                 help="指定配置文件")

    def get_last_date(self):
        last_date = datetime.datetime.now() - datetime.timedelta(days=1)
        return last_date.strftime("%Y-%m-%d")

    def parse_args(self):
        args = self.parser.parse_args()
        # options, _ = self.parser.parse_args()
        options = args
        print(args)
        if options.module in self.func.keys():
            if options.config:
                with open(options.config, 'r') as f:
                    args = json.load(f)
                    if "capture" in options.module:
                        pass
                        # cmdb_id = args.get("cmdb_id")
                        # sql = "select ip_address, port, connect_name, service_name from T_CMDB where cmdb_id = :1"
                        # host, port, sid, username, password
                        # capture_date = args.get("capture_date")
                        # flag_type = args.get("type")
                        # self.func[options.module](db_host, db_port, sid, username, password, capture_date, flag_type)
                    elif "analysis" in options.module:
                        self.func[options.module](**args)
                    else:
                        self.func[options.module]()
            else:
                self.parser.print_help()
                sys.exit(0)
        else:
            self.parser.print_help()
            sys.exit(0)

    def run_capture(self, host, port, sid, username, password, capture_date, flag_type, connect_name, record_id, cmdb_id):
        """
            运行数据抓取模块，主要针对oracle，mysql依赖于pt-query-digest
            flag_type: OBJ, OTHER
        """
        db_client = past.rule_analysis.db.db_operat.DbOperat(host=host, port=port, username=username, password=password, db=sid)
        print("After db_client initial.....")
        if flag_type == "OBJ":
            past.capture.sql_obj_info.CaptureObj(
                self.mongo_client,
                db_client.get_db_cursor(),
                arrow.get(capture_date).datetime,
                host,
                sid=sid,
                record_id=record_id,
                cmdb_id=cmdb_id
            ).run()
        elif flag_type == "OTHER":
            startdate = " ".join([capture_date, "00:00:00"])
            stopdate = " ".join([capture_date, "23:59:59"])
            past.capture.sql_other_info.CaptureOther(
                self.mongo_client,
                db_client.get_db_cursor(),
                ipaddress=host,
                sid=sid,
                etl_date=capture_date,
                startdate=startdate,
                stopdate=stopdate,
                record_id=record_id,
                cmdb_id=cmdb_id
            ).run()

    def run_analysis(self, **args):
        """
        规则解析模块，支持两种数据库，四种规则类型，主要是对SqlAudit类的封装
        """
        kwargs = {
            "mongo_server": settings.MONGO_SERVER,
            "mongo_port": settings.MONGO_PORT,
            "mongo_db": settings.MONGO_DB,
            "mongo_user": settings.MONGO_USER,
            "mongo_password": settings.MONGO_PASSWORD,
            "record_id": args.get("record_id"),
        }
        record_id = args.get("record_id")
        username = args.get("username")
        rule_type = args.get("rule_type")
        rule_status = args.get("rule_status", "ON")
        db_type = args.get("db_type")  # O: oracle, mysql: mysql
        create_user = args.get("create_user")  # ISQLAUDIT
        cmdb_id = args.get("cmdb_id")
        connect_name = args.get("connect_name")

        sql = "SELECT db_model FROM T_CMDB WHERE cmdb_id = :1"
        db_model = plain_db.oracleob.OracleHelper.select_dict(sql, [cmdb_id], one=True)['db_model']
        kwargs['db_model'] = db_model

        sid = args.get("sid")
        login_user = args.get("login_user")  # schema
        password = args.get("password")
        start_date = past.utils.utils.get_time()
        if rule_type == "OBJ":
            kwargs.update({
                "db_server": args.get("db_server"),
                "db_port": args.get("db_port"),
                "db_user": login_user,
                "db_passwd": password,
                "db": sid,
                "connect_name": connect_name
            })
            instance_name = kwargs.get("db")
            sqlaudit = past.rule_analysis.sqlaudit.SqlAudit(username, rule_type, rule_status, db_type,
                                create_user=create_user, **kwargs)
            job_record,rule_name_and_len = sqlaudit.run()
        elif db_type == utils.const.DB_ORACLE and rule_type in ["SQLPLAN", "SQLSTAT"]:
            instance_name = args.get("sid")
            capture_date = args.get("capture_date")
            sqlaudit = past.rule_analysis.sqlaudit.SqlAudit(username, rule_type, rule_status, db_type,
                                startdate=capture_date, create_user=create_user,
                                **kwargs)
            job_record,rule_name_and_len = sqlaudit.run()
        elif rule_type == "TEXT":
            startdate = args.get("startdate")
            stopdate = args.get("stopdate")
            if db_type == utils.const.DB_ORACLE:
                instance_name = args.get("sid")
                hostname = args.get("hostname")
            # elif db_type == "mysql":
            #     instance_name = "mysql"
            #     hostname = args.get("hostname_max")
            #     temp = {
            #         "db_server": settings.PT_QUERY_SERVER,
            #         "db_port": settings.PT_QUERY_PORT,
            #         "db_user": settings.PT_QUERY_USER,
            #         "db_passwd": settings.PT_QUERY_PASSWD,
            #         "db": settings.PT_QUERY_DB
            #     }
            #     kwargs.update({"pt_server_args": temp})
            sqlaudit = past.rule_analysis.sqlaudit.SqlAudit(username, rule_type, rule_status, db_type,
                                startdate=startdate, stopdate=stopdate,
                                create_user=create_user, hostname=hostname,
                                **kwargs)
            job_record,rule_name_and_len = sqlaudit.run()
        # elif db_type == "mysql" and rule_type in ["SQLPLAN", "SQLSTAT"]:
        #     instance_name = "mysql"
        #     startdate = args.get("startdate")
        #     stopdate = args.get("stopdate")
        #     temp = {
        #         "db_server": settings.PT_QUERY_SERVER,
        #         "db_port": settings.PT_QUERY_PORT,
        #         "db_user": settings.PT_QUERY_USER,
        #         "db_passwd": settings.PT_QUERY_PASSWD,
        #         "db": settings.PT_QUERY_DB
        #     }
        #     kwargs.update({"pt_server_args": temp})
        #     sqlaudit = SqlAudit(username, rule_type, rule_status, db_type,
        #                         startdate=startdate, stopdate=stopdate,
        #                         create_user=create_user, **kwargs)
        #     db_server = args.get("db_server")
        #     db_port = args.get("db_port")
        #     hostname_max = ":".join([db_server, str(db_port)])
        #     job_args = {
        #         "user": settings.MYSQL_ACCOUNT[hostname_max][1],
        #         "passwd": settings.MYSQL_ACCOUNT[hostname_max][2],
        #         "hostname": hostname_max
        #     }
        #     job_record = sqlaudit.run(**job_args)
        task_ip = args.get("task_ip")
        task_port = args.get("task_port")
        self.save_result(
            sqlaudit,
            job_record,
            create_user,
            instance_name,
            task_ip,
            task_port,
            cmdb_id,
            username,
            connect_name,
            start_date,
            record_id,
            rule_type,
            rule_name_and_len
        )

    def save_result(
            self,
            sqlaudit,
            job_record,
            create_user,
            instance_name,
            task_ip,
            task_port,
            cmdb_id,
            username,
            connect_name,
            start_date,
            record_id,
            rule_type,
            rule_name_and_len
    ):
        args = {
            'operator_user': create_user,
            'startdate': start_date,
            'stopdate': past.utils.utils.get_time(),
            'instance_name': instance_name,
            'task_ip': task_ip,
            'task_port': task_port,
            'cmdb_id': cmdb_id,
            'connect_name': connect_name,
            'record_id': record_id,
        }
        sqlaudit.review_result.job_init(**args)
        result_update_info = {
            "task_uuid": sqlaudit.review_result.task_id,
            'cmdb_id': cmdb_id,
            'schema_name': username,
            'create_date': past.utils.utils.get_time(),
            'etl_date': past.utils.utils.get_time(),
            'ip_address': task_ip,
            'sid': instance_name,
            'record_id': record_id,
            'rule_type': rule_type,
        }
        job_record.update(result_update_info)
        try:
            sqlaudit.mongo_client.insert_one("results", job_record)
            models.mongo.Job.objects(id=sqlaudit.review_result.task_id).update(
                set__status=utils.const.JOB_STATUS_FINISHED,
                set__desc__capture_time_end=past.utils.utils.get_time()
            )

        except DocumentTooLarge:
            # print(e)
            pt=PrettyTable(rule_name_and_len.keys())
            pt.add_row(rule_name_and_len.values())
            print(pt)

        # sql = {'_id': sqlaudit.review_result.task_id}
        # condition = {"$set": {"status": 1, "desc.capture_time_end": past.utils.utils.get_time()}}
        # sqlaudit.mongo_client.update_one("job", sql, condition)

    def run(self):
        self.parse_init()
        self.parse_args()


if __name__ == "__main__":
    com = Command()
    com.run()
    print("finish...")

