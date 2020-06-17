
import arrow
import yagmail
import traceback

import task.const
from task.task import *
from models.sqlalchemy import make_session
from .mail import *



@register_task(task.const.TASK_TYPE_MAIL)
class SendMialREPORT(BaseTask):
    """发送邮件报告"""

    @classmethod
    def mail_server(cls):
        with make_session() as session:
            m_s=session.query(MailServer).first()
            host = m_s['host']
            port = m_s['port']
            user = m_s['user']
            password = m_s['password']
            smtp_ssl = m_s['smtp_ssl']
            smtp_skip_login = False if m_s['password'] else True#跳过登录开启不需要输入密码
            return host,port,user,password,smtp_ssl,smtp_skip_login

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        report_item_list: list = kwargs["report_item_list"]
        recipient_list: list = kwargs["recipient_list"]
        receive_mail_id: int =kwargs["receive_mail_id"]
        host,port,user,password,smtp_ssl,smtp_skip_login = cls.mail_server()

        #send
        last_send_status=True
        last_error_msg=""
        try:
            yag = yagmail.SMTP(user=user,password=password,
                               host=host,port=port,smtp_ssl=smtp_ssl,
                               smtp_skip_login=smtp_skip_login)
            report_subject=",".join(report_item_list)
            yag.send(to=recipient_list,subject=report_subject,
                     attachments=None)
        except Exception as error:
            print(traceback.format_exc())
            last_send_status = False
            last_error_msg = str(error.__str__())
        print(last_send_status, last_error_msg)


        #update
        with make_session() as session:
            last_send_time=arrow.now()
            session.query(ReceiveMailInfoList).\
                filter_by(receive_mail_id=receive_mail_id).\
                update(last_send_status=last_send_status,
                       last_error_msg=last_error_msg,
                       last_send_time=last_send_time)

            recipient_list = ";".join(recipient_list)
            report_item_list = ";".join(report_item_list)
            rmh=ReceiveMailHistory()
            rmh.receive_mail_id=receive_mail_id
            rmh.report_item_list=report_item_list
            rmh.recipient_list=recipient_list
            rmh.send_status=last_send_status
            rmh.send_time=last_send_time
            rmh.error_msg=last_error_msg
            rmh.report_path=None
            session.add(rmh)
            session.commit()