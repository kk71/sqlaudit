
from auth.mail import *

from models.sqlalchemy import make_session


def send_mail(send_user_list):
    """发送邮件"""

    with make_session() as session:
        mail_server=session.query(MailServer).first()
    for send_user_