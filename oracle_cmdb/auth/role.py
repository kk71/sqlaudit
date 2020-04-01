# Author: kk.Fang(fkfkbill@gmail.com)


class RoleDataPrivilege(BaseModel):
    """角色数据库权限"""
    __tablename__ = "T_ROLE_DATA_PRIVILEGE"

    id = Column("ID", Integer, Sequence("SEQ_ROLE_DATA_PRIVILEGE_ID"), primary_key=True)
    role_id = Column("ROLE_ID", Integer)
    cmdb_id = Column("CMDB_ID", Integer)
    schema_name = Column("SCHEMA_NAME", String)
    comments = Column("COMMENTS", String)
    create_date = Column("CREATE_DATE", DATE, default=datetime.now)

