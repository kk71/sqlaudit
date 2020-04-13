# Author: kk.Fang(fkfkbill@gmail.com)

from typing import Union


# 系统权限

class PRIVILEGE:
    # 权限执行方
    TYPE_BE = 1  # 仅后端处理
    TYPE_FE = 2  # 仅前端处理
    TYPE_BOTH = 3  # 皆处理
    ALL_PRIVILEGE_TYPE = (TYPE_BE, TYPE_FE, TYPE_BOTH)

    # 权限键名
    NAMES = ("id", "type", "name", "description")
    ALL_PRIVILEGE = (
        # 新增权限的时候，请保持id不断增大，不要复用旧id，哪怕已经删掉的权限的id也不要用)
        # 删除权限请全代码搜索删除
        # PRIVILEGE_DASHBOARD = (1, TYPE_FE, "仪表盘", "是否允许使用")
        # PRIVILEGE_SQL_HEALTH = (2, TYPE_FE, "SQL健康度", "")
        PRIVILEGE_ONLINE := (3, TYPE_FE, "线上审核", ""),
        PRIVILEGE_OFFLINE := (4, TYPE_FE, "线下审核", ""),
        PRIVILEGE_SELF_SERVICE_ONLINE := (5, TYPE_BOTH, "自助上线", ""),
        PRIVILEGE_SQL_TUNE := (6, TYPE_FE, "智能优化", ""),
        PRIVILEGE_USER_MANAGER := (7, TYPE_FE, "用户管理", ""),
        PRIVILEGE_CMDB := (8, TYPE_FE, "纳管数据库管理", ""),
        PRIVILEGE_TASK := (9, TYPE_FE, "任务管理", ""),
        PRIVILEGE_RULE := (10, TYPE_FE, "规则编辑", ""),
        PRIVILEGE_SIMPLE_RULE := (11, TYPE_FE, "增加简单规则", "是否允许使用"),
        # PRIVILEGE_COMPLEX_RULE = (12, TYPE_FE, "增加复杂规则", "是否允许使用"),
        PRIVILEGE_WHITE_LIST := (13, TYPE_FE, "风险白名单管理", "是否允许使用"),
        PRIVILEGE_RISK_RULE := (14, TYPE_FE, "风险SQL规则管理", "是否允许使用"),
        PRIVILEGE_MAIL_SEND := (15, TYPE_FE, "报告发送管理", "是否允许使用"),
        PRIVILEGE_METADATA := (16, TYPE_FE, "元数据", "是否允许使用"),
        PRIVILEGE_OFFLINE_TICKET_APPROVAL := (17, TYPE_BOTH, "审批线下工单", ""),
        PRIVILEGE_OFFLINE_TICKET_ADMIN := (18, TYPE_BE, "查看全部线下工单", ""),
        PRIVILEGE_ROLE_MANAGE := (19, TYPE_FE, "角色管理", ""),
        PRIVILEGE_ROLE_USER_MANAGE := (20, TYPE_FE, "用户角色管理", ""),
        PRIVILEGE_ROLE_DATA_PRIVILEGE := (21, TYPE_FE, "数据权限配置", "是否允许使用"),
        PRIVILEGE_HEALTH_CENTER := (22, TYPE_FE, "健康中心", ""),
        PRIVILEGE_TICKET_RULE := (23, TYPE_BOTH, "工单规则", "")
    )

    @classmethod
    def privilege_to_dict(cls, x):
        return dict(zip(cls.NAMES, x))

    @classmethod
    def get_privilege_by_id(cls, privilege_id) -> Union[tuple, None]:
        """
        根据权限id获取权限tuple
        :param privilege_id:
        :return: 注意当权限不存在的时候会返回None，所以务必对返回的东西作判断
        """
        for i in cls.ALL_PRIVILEGE:
            if i[0] == privilege_id:
                return i

    @classmethod
    def get_privilege_by_type(cls, privilege_type) -> list:
        if not isinstance(privilege_type, (tuple, list)):
            privilege_type = (privilege_type,)
        return [i for i in cls.ALL_PRIVILEGE if i[1] in privilege_type]

    @classmethod
    def get_all_privilege_id(cls) -> [int]:
        return [i[0] for i in cls.ALL_PRIVILEGE]


# mail发送时间
ALL_SEND_DATE = ("星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日")
ALL_SEND_TIME = ("0:00", "1:00", "2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00",
                 "9:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00",
                 "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00")
