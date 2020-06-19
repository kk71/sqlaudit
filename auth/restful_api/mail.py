from .base import *
from auth.const import *
from auth.mail import *
from ..const import PRIVILEGE
from ..tasks_mail import SendMialREPORT
from utils.schema_utils import *
from models.sqlalchemy import *
from restful_api.modules import *
from sqlalchemy import desc


@as_view("receive_mail_info_list", group="mail")
class ReceiveMailInfoListHandler(PrivilegeReq):

    def get(self):
        """报告发送收件信息管理"""

        self.acquire(PRIVILEGE.PRIVILEGE_MAIL_SEND)

        params = self.get_query_args(Schema({
            scm_optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        del params

        with make_session() as session:
            receive_mail_info_q = session.query(ReceiveMailInfoList)
            if keyword:
                receive_mail_info_q = self.query_keyword(receive_mail_info_q, keyword,
                                                    ReceiveMailInfoList.report_item_list,
                                                    ReceiveMailInfoList.recipient_list,
                                                    ReceiveMailInfoList.send_date,
                                                    ReceiveMailInfoList.send_time)
            receive_mail_info_q, p = self.paginate(receive_mail_info_q, **p)
            receive_mail_info = [x.to_dict() for x in receive_mail_info_q]
            self.resp(receive_mail_info, **p)

    get.argument = {
        "querystring": {
            "//keyword": "",
            "page": 1,
            "per_page": 10
        }
    }

    def post(self):
        """新增收件信息"""
        params = self.get_json_args(Schema({
            "report_item_list": [scm_str],#TODO
            "recipient_list" : [scm_str],
            "send_date": scm_one_of_choices(ALL_SEND_DATE),
            "send_time": scm_one_of_choices(ALL_SEND_TIME)
        }))
        recipient_list = params.pop("recipient_list")
        recipient_list = ";".join(recipient_list)
        report_item_list = params.pop("report_item_list")
        report_item_list = ";".join(report_item_list)

        with make_session() as session:
            rmi= ReceiveMailInfoList(**params)
            rmi.recipient_list = recipient_list
            rmi.report_item_list = report_item_list
            session.add(rmi)
            session.commit()
            session.refresh(rmi)
            self.resp_created(rmi.to_dict(), msg="添加收件人成功")

    post.argument = {
        "json": {
            "report_item_list": ["风险sql报告","风险对象报告","schema报告"],
            "recipient_list" : ["574691837@qq.com"],
            "send_date": "星期一",
            "send_time": "0:00"
        }
    }

    def patch(self):
        """编辑收件信息"""
        params = self.get_json_args(Schema({
            "receive_mail_id": scm_int,

            "report_item_list": [scm_str],# TODO
            "recipient_list": [scm_str],
            "send_date": scm_one_of_choices(ALL_SEND_DATE),
            "send_time": scm_one_of_choices(ALL_SEND_TIME)
        }))

        receive_mail_id = params.pop("receive_mail_id")
        recipient_list = params.pop("recipient_list")
        recipient_list = ";".join(recipient_list)
        report_item_list = params.pop("report_item_list")
        report_item_list = ";".join(report_item_list)

        with make_session() as session:
            session.query(ReceiveMailInfoList). \
                filter_by(receive_mail_id=receive_mail_id).\
                update(dict(report_item_list=report_item_list,
                       recipient_list=recipient_list,
                       **params))
            self.resp_created(msg="finished")

    patch.argument = {
        "json": {
            "receive_mail_id":"",
            "report_item_list": ["风险sql报告","风险对象报告","schema报告"],
            "recipient_list" : ["qq@qq.com", "574691837@qq.com"],
            "send_date": "星期一",
            "send_time": "2:00"
        }
    }

    def delete(self):
        """删除收件信息"""
        params = self.get_json_args(Schema({
            "receive_mail_id": scm_int
        }))
        receive_mail_id = params.pop("receive_mail_id")
        with make_session() as session:
            session.query(ReceiveMailInfoList). \
                filter_by(receive_mail_id=receive_mail_id). \
                delete(synchronize_session=False)
            self.resp_created("删除列表成功")

    delete.argument = {
        "json": {
            "receive_mail_id": ""
        }
    }


@as_view("receive_mail_history", group="mail")
class ReceiveMailHistoryHandler(AuthReq):

    def get(self):
        """某个接收邮件历史记录列表"""
        params = self.get_query_args(Schema({
            "receive_mail_id": scm_int,

            **self.gen_p()
        }))
        p = self.pop_p(params)
        receive_mail_id = params.pop("receive_mail_id")

        with make_session() as session:
            receive_mail_history_q = session.query(ReceiveMailHistory). \
                filter(ReceiveMailHistory.receive_mail_id == receive_mail_id). \
                order_by(desc("send_time"))
            receive_mail_history_q, p = self.paginate(receive_mail_history_q, **p)
            self.resp([x.to_dict() for x in receive_mail_history_q], **p)

    get.argument = {
        "querystring": {
            "receive_mail_id": "",
            "page":1,
            "per_page":10
        }
    }


@as_view("mail_server", group="mail")
class MailServerHandler(AuthReq):

    def get(self):
        """邮件服务器配置获取"""
        with make_session() as session:
            mail_server = session.query(MailServer).first()
            if mail_server:
                self.resp(mail_server.to_dict())
            else:
                self.resp(content={})

    get.argument = {
        "querystring": {}
    }

    def patch(self):
        """邮件服务器配置修改"""
        params = self.get_json_args(Schema({
            "mail_server_name": scm_unempty_str,
            "user": scm_unempty_str,
            "host": scm_unempty_str,
            "port": scm_int,
            "smtp_ssl": scm_bool,
            "smtp_skip_login": scm_bool,
            scm_optional("password"): scm_empty_as_optional(scm_str),
            scm_optional("comments", default=None): scm_str
        }))

        with make_session() as session:
            mail_server = session.query(MailServer).first()
            if mail_server:
                session.query(MailServer). \
                    filter_by(mail_server_id=mail_server.mail_server_id).update(params)
            else:
                mail_server = MailServer(**params)
                session.add(mail_server)
                session.commit()
                session.refresh(mail_server)
            self.resp_created(mail_server.to_dict(), msg="修改发件人配置成功")

    patch.argument = {
        "json": {
            "mail_server_name": "xx...",
            "user": "m15081369391@163.com",
            "host": "smtp.163.com",
            "port": "25",
            "smtp_ssl": False,
            "smtp_skip_login": True,
            "//password": "302435hyj",
            "//comments": ""
        }
    }


@as_view("send_test_email", group="mail")
class SendTestEmailHandler(AuthReq):

    async def post(self):
        """发送测试邮件"""
        params = self.get_json_args(Schema({
            "receive_mail_id": scm_int,
            "recipient_list": [scm_str],
        }))
        receive_mail_id = params.pop("receive_mail_id")
        with make_session() as session:
            rmil=session.query(ReceiveMailInfoList).filter_by(receive_mail_id=receive_mail_id).first()
            parame_dict={"report_item_list" :rmil.report_item_list,
                         "receive_mail_id":receive_mail_id,
                         **params}

            await SendMialREPORT.async_shoot(async_task_timeout=60,**parame_dict)
            await self.resp_created(msg="邮件正在发送, 请注意过一会查收")

    post.argument = {
        "json": {
            "receive_mail_id": "2",
            "recipient_list": ["574691837@qq.com"],
        }
    }


