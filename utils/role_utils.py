# Author: kk.Fang(fkfkbill@gmail.com)


ROLE_ADMIN = 1
ROLE_DBA = 2
ROLE_DEVELOPER = 3


ALL_ROLES = (
    ROLE_ADMIN,
    ROLE_DBA,
    ROLE_DEVELOPER
)


def get_dba_ids(session, q):
    """
    获取可以审批线下审核的人名单列表
    :param session:
    :param q:
    :return:
    """
    # TODO 不知道哪些人可以审批那些人不能，所以默认不过滤
    return q
