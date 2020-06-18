import yagmail
import traceback
import os, zipfile,time

import settings
import task.const
from .mail import *
from .user import *
from task.task import *
from models.sqlalchemy import make_session
from oracle_cmdb.auth.user_utils import current_cmdb,current_schema
from utils.datetime_utils import *
from oracle_cmdb.restful_api.risk_rule import GetRiskRuleBase, RiskRuleSql,RiskRuleObj
from oracle_cmdb.tasks.risk_sql_export import RiskRuleSqlExport
from oracle_cmdb.tasks.risk_obj_export import RiskRuleObjExport
from oracle_cmdb.tasks.capture import OracleCMDBTaskCapture
from oracle_cmdb.restful_api.health_center import SchemaIssueRuleBase, OutputDataBase
from oracle_cmdb.tasks.schema_report_export import SchemaReportExport
from .const import *

def get_zip_file(input_path, result):
    files = os.listdir(input_path)
    for file in files:
        if os.path.isdir(input_path + '/' + file):
            get_zip_file(input_path + '/' + file, result)
        else:
            result.append(input_path + '/' + file)

def zip_file_path(input_path, output_path, output_name):
    """
     :param input_path: The path to the generated file
     :param output_path: The path to place the file
     :param output_name: The name of the package file
   """
    target_filepath = os.path.join(output_path, output_name)
    f = zipfile.ZipFile(target_filepath, 'w', zipfile.ZIP_DEFLATED)
    filelists = []
    get_zip_file(input_path, filelists)
    for dirpath, dirnames, filenames in os.walk(input_path):
        for filename in filenames:
            f.write(os.path.join(dirpath, filename), filename)
    f.close()
    ret_url = os.path.join(settings.MAIL_DIR, output_name)
    return ret_url


@register_task(task.const.TASK_TYPE_MAIL)
class SendMialREPORT(BaseTask):
    """发送邮件报告"""

    @classmethod
    def mail_server(cls):
        with make_session() as session:
            m_s=session.query(MailServer).first()
            host = m_s.host
            port = m_s.port
            user = m_s.user
            password = m_s.password
            smtp_ssl = m_s.smtp_ssl
            smtp_skip_login = False if m_s.password else True#跳过登录开启不需要输入密码
            return host,port,user,password,smtp_ssl,smtp_skip_login

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        report_item_list: str = kwargs["report_item_list"]
        recipient_list: list = kwargs["recipient_list"]
        receive_mail_id: int =kwargs["receive_mail_id"]
        host,port,mail_user,password,smtp_ssl,smtp_skip_login = cls.mail_server()

        with make_session() as session:
            users=session.query(User).filter(User.email.in_(recipient_list)).all()

            for user in users:
                path_prefix = "/tmp/" + str(int(time.time()))
                if not os.path.exists(path_prefix):
                    os.makedirs(path_prefix)
                print(path_prefix)

                cmdb_ids=current_cmdb(session,user.login_user)

                for cmdb_id in cmdb_ids:
                    if RISK_SQL_REPORT in report_item_list:
                        date_start = str(arrow.now().date())
                        date_end = str(arrow.now().shift(days=+1).date())
                        entry ="SQL"
                        params={"cmdb_id":cmdb_id,"date_start":date_start,"date_end":date_end,"entry":entry}
                        risk_rule_outer, cmdb_id, task_record_id_list=GetRiskRuleBase().get_risk_rule(session,**params)
                        parame_dict = RiskRuleSql().risk_rule_sql_inner(risk_rule_outer,cmdb_id,task_record_id_list)
                        filename=f"risk_rule_sql_{cmdb_id}_{dt_to_str(arrow.now())}.xlsx"
                        RiskRuleSqlExport.report(path_prefix,filename,parame_dict=parame_dict)
                    if RISK_OBJ_REPORT in report_item_list:
                        entry="OBJECT"
                        params = {"cmdb_id": cmdb_id, "date_start": date_start, "date_end": date_end, "entry": entry}
                        risk_rule_outer, cmdb_id, task_record_id_list = GetRiskRuleBase().get_risk_rule(session, **params)
                        risk_rule_obj_inner = RiskRuleObj().get_risk_obj(cmdb_id=cmdb_id,task_record_id__in = task_record_id_list)
                        parame_dict = {
                            "risk_rule_outer": risk_rule_outer,
                            "risk_rule_obj_inner": risk_rule_obj_inner
                        }
                        filename = f"risk_rule_obj_{cmdb_id}_{dt_to_str(arrow.now())}.xlsx"
                        RiskRuleObjExport.report(path_prefix, filename, parame_dict=parame_dict)
                    if SCHEMA_REPORT in report_item_list:
                        schemas: list=current_schema(session,login_user=user.login_user,cmdb_id=cmdb_id)
                        cmdb_last_success_task_record_id:dict=OracleCMDBTaskCapture.last_success_task_record_id_dict(session,cmdbs=cmdb_id)
                        task_record_id = cmdb_last_success_task_record_id[cmdb_id]
                        for schema in schemas:
                            params={"cmdb_id":cmdb_id,"task_record_id":task_record_id,"schema_name":schema}
                            schema_issue_rule_dict,_,_,_=SchemaIssueRuleBase().schema_issue_rule(**params)
                            output_data = OutputDataBase().get_output_data(cmdb_id,task_record_id,schema)
                            filename = f"schema_report_{cmdb_id}_{schema}_{dt_to_str(arrow.now())}.xlsx"
                            parame_dict = {
                                "schema_issue_rule_dict": schema_issue_rule_dict,
                                "output_data": output_data
                            }
                            SchemaReportExport.report(path_prefix, filename,parame_dict=parame_dict)

                zip_name=f"mail_report_{user.login_user}.zip"
                attachments_path=zip_file_path(path_prefix,settings.MAIL_DIR,zip_name)
                if not cmdb_ids:
                    attachments_path=None#此接收邮件用户无绑定任何库(邮件发送成功无附加是用户没纳管库或纳管后今天没采集)
                #send
                last_send_status=True
                last_error_msg=""
                try:
                    yag = yagmail.SMTP(user=mail_user,password=password,
                                       host=host,port=port,smtp_ssl=smtp_ssl,
                                       smtp_skip_login=smtp_skip_login)
                    yag.send(to=user.email,subject=report_item_list,
                             attachments=attachments_path)
                except Exception as error:
                    print(traceback.format_exc())
                    last_send_status = False
                    last_error_msg = str(error.__str__())
                print(last_send_status, last_error_msg)

                #update insert data
                last_send_time=arrow.now().datetime
                session.query(ReceiveMailInfoList).\
                    filter_by(receive_mail_id=receive_mail_id).\
                    update({"last_send_status":last_send_status,
                           "last_error_msg":last_error_msg,
                           "last_send_time":last_send_time})

                rmh=ReceiveMailHistory()
                rmh.receive_mail_id=receive_mail_id
                rmh.report_item_list=report_item_list
                rmh.recipient_list=user.email
                rmh.send_status=last_send_status
                rmh.send_time=last_send_time
                rmh.error_msg=last_error_msg
                rmh.report_path=os.path.join(settings.EXPORT_PREFIX_MAIL,zip_name)
                session.add(rmh)
                session.commit()