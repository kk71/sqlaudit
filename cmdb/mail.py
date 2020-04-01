# Author: kk.Fang(fkfkbill@gmail.com)

class SendMailList(BaseModel):
    __tablename__ = "SEND_MAIL_LIST"

    send_mail_id = Column("SEND_MAIL_ID", Integer, Sequence("SEQ_SEND_MAIL_LIST"), primary_key=True)
    title = Column("TITLE", String)
    contents = Column("CONTENTS", String)
    mail_sender = Column("MAIL_SENDER", String)
    send_date = Column("SEND_DATE", String)
    send_time = Column("SEND_TIME", String)
    send_content_item = Column("SEND_CONTENT_ITEM", String)
    last_send_status = Column("LAST_SEND_STATUS", Boolean)
    last_send_date = Column("LAST_SEND_DATE", DATE, default=datetime.now)
    error_msg = Column("ERROR_MSG", String)


class MailServer(BaseModel):
    __tablename__ = "MAIL_SERVER"

    mail_server_id = Column("MAIL_SERVER_ID", Integer, Sequence("SEQ_MAIL_SERVER"), primary_key=True)
    mail_server_name = Column("MAIL_SERVER_NAME", String)
    ip_address = Column("IP_ADDRESS", String)
    protocol = Column("PROTOCOL", String)
    port = Column("PORT", Integer)
    username = Column("USERNAME", String)
    password = Column("PASSWORD", String)
    status = Column("STATUS", Boolean)
    comments = Column("COMMENTS", String)
    use_ssl = Column("USESSL", Boolean)


class SendMailHist(BaseModel):
    __tablename__ = "T_SEND_MAIL_HIST"

    id = Column("ID", Integer, Sequence("SEQ_SEND_MAIL_HIST"), primary_key=True)
    send_mail_list_id = Column("SEND_MAIL_LIST_ID", Integer)
    receiver = Column("receiver", String)
    file_path = Column("FILE_PATH", String)
    create_time = Column("CREATE_TIME", DATE, default=datetime.now)
    status = Column("STATUS", Boolean)
