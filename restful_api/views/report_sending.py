from schema import Schema, Optional

from .base import AuthReq
from models.oracle import *
from utils.schema_utils import *
from utils.const import *


class ManageHandler(AuthReq):

    def get(self):
        """报告发送管理页面"""
        params = self.get_query_args(Schema({
            Optional("keyword", default=None): scm_unempty_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        del params

        login_user = self.current_user

        with make_session() as session:
            # 报告发送详情列表
            send_mail_data = session.query(SendMailList)

            if keyword:
                send_mail_data = self.query_keyword(send_mail_data, keyword,
                                                    SendMailList.title,
                                                    SendMailList.contents,
                                                    SendMailList.mail_sender)
            send_mail_data, p = self.paginate(send_mail_data, **p)
            send_mail_data = [x.to_dict() for x in send_mail_data]

            # # 报告内容
            # report_content = {x['send_mail_id']: x['title'] for x in send_mail_data}
            #
            # #收件人邮箱列表
            # users_mail_data=session.query(User).with_entities(User.email)
            # users_mail_data=[x[0] for x in users_mail_data]
            #
            #
            # #收件人类型姓名
            # q=QueryEntity(Role.role_name,User.user_name,User.login_user)
            # user_type_name_list=session.query(*q).join(UserRole,User.login_user==UserRole.login_user).\
            #     join(Role,Role.role_id==UserRole.role_id).filter(User.login_user !="admin")
            # user_type_name_list = [q.to_dict(x) for x in user_type_name_list]
            # for user in user_type_name_list:
            #     if user["role_name"]=="administrator":
            #         user["role_name"]=TYPE_ADMINISTRATOR
            #     elif user["role_name"]=="dba":
            #         user["role_name"]=TYPE_DBA
            #     elif user["role_name"]=="operator":
            #         user["role_name"]=TYPE_OPERATOR
            #     else:
            #         pass

            self.resp({'send_mail_data': send_mail_data,
                       # "users_mail_data": users_mail_data,
                       # "user_type_name_list":user_type_name_list,
                       "login_user": login_user,
                       # **report_content,
                       **p

                       })

    def post(self):
        params = self.get_json_args(Schema({
            "title": scm_str,
            "contents": scm_str,
            "mail_sender": [scm_str],  # 收件人
            "send_date": scm_one_of_choices(ALL_SEND_DATE),
            "send_time": scm_one_of_choices(ALL_SEND_TIME),
            # "send_content_item":scm_unempty_str,
        }))
        mail_sender = params.pop("mail_sender")
        mail_sender = ";".join(mail_sender)

        with make_session() as session:
            s = SendMailList(**params)
            s.mail_sender = mail_sender
            session.add(s)
            session.commit()
            session.refresh(s)

            self.resp_created(s.to_dict(), msg="添加收件人成功")

    def patch(self):
        params = self.get_json_args(Schema({
            "send_mail_id": scm_int,

            "title": scm_str,
            "contents": scm_str,
            "mail_sender": [scm_str],  # 收件人
            "send_date": scm_one_of_choices(ALL_SEND_DATE),
            "send_time": scm_one_of_choices(ALL_SEND_TIME),
            "send_content_item": scm_unempty_str,
        }))
        mail_sender = params.pop("mail_sender")
        mail_sender = ";".join(mail_sender)
        mail_sender = {"mail_sender": mail_sender}
        send_mail_id = params.pop("send_mail_id")
        with make_session() as session:
            w = session.query(SendMailList). \
                filter_by(send_mail_id=send_mail_id).update(dict(mail_sender, **params))

            self.resp_created(msg="")  # TODO

    def delete(self):
        params = self.get_query_args(Schema({
            "send_mail_id": scm_int
        }))
        with make_session() as session:
            session.query(SendMailList).filter(SendMailList.send_mail_id == params['send_mail_id']).delete()

            self.resp_created("删除列表成功")


class ConfigSenderHandler(AuthReq):
    """发件人邮件服务器配置"""

    def get(self):
        with make_session() as session:
            server_data = session.query(MailServer).first()

            self.resp(server_data.to_dict())

    def patch(self):
        params = self.get_json_args(Schema({
            "mail_server_name": scm_unempty_str,
            'usessl': scm_bool,
            # 'ip_address':scm_str,
            # 'protocol':scm_unempty_str,
            "port": scm_int,
            "username": scm_unempty_str,
            "password": scm_unempty_str,
            "status": scm_bool,
            Optional("comments", default=None): scm_str

        }))

        with make_session() as session:
            mailserver = session.query(MailServer).first()
            session.query(MailServer). \
                filter_by(mail_server_id=mailserver.mail_server_id).update(params)

            self.resp_created(mailserver.to_dict(), msg="修改发件人配置成功")


class SendMailHandler(AuthReq):
    """发送测试邮件"""

    def post(self):
        params = self.get_json_args(Schema({
            "send_mail_id": scm_int,
            "user_type_name_list": [scm_str],
        }))
        send_mail_id = params.pop("send_mail_id")

        with make_session() as session:
            q = QueryEntity(SendMailList.title, SendMailList.contents, SendMailList.send_mail_id)
            send_mail = session.query(*q).filter_by(send_mail_id=send_mail_id)
            send_mail = [q.to_dict(x) for x in send_mail]
            for x in send_mail:
                x.update({**params})
            # TODO
            # timing_send_email.delay(send_mail)

            self.resp_created(send_mail, msg="发送正在发送邮件, 请注意查收")


class MailHistory(AuthReq):
    """某个邮件历史发送记录列表"""

    def get(self):
        params = self.get_query_args(Schema({
            "send_mail_id": scm_int,

            Optional("keyword", default=None): scm_str,
            **self.gen_p()
        }))
        keyword = params.pop("keyword")
        p = self.pop_p(params)
        send_mail_id = params["send_mail_id"]
        del params

        with make_session() as session:

            users_data = session.query(User).with_entities(User.login_user, User.user_name)
            users_data = {x[0]: x[1] for x in users_data}

            mail_hist = session.query(SendMailHist). \
                filter_by(send_mail_list_id=send_mail_id).order_by(SendMailHist.id)
            mail_hist = [x.to_dict() for x in mail_hist]

            mail_list = session.query(SendMailList).filter_by(send_mail_id=send_mail_id)

            if keyword:
                mail_list = self.query_keyword(mail_list, keyword,
                                               SendMailList.title,
                                               SendMailList.contents)
            mail_list = [x.to_dict() for x in mail_list]

            data = []
            for x in mail_hist:
                if x["receiver"] not in users_data.keys():
                    continue
                res = "失败"
                if x['status'] == 1:
                    res = "成功"

                download_path = '/downloads/' + 'O18 SQL审核报告_' + users_data[x["receiver"]] + '_' + \
                                x["create_time"] + '.zip'

                if mail_list:  # 搜索的兼容性
                    data.append({
                        'title': mail_list[0]['title'],
                        'contents': mail_list[0]['contents'],
                        'receiver': users_data[x["receiver"]],
                        'create_time': x['create_time'],
                        'status': res,
                        "download_path": download_path,
                    })

            data, p = self.paginate(data, **p)

            self.resp(data, **p)