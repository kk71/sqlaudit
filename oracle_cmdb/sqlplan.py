# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseOracleSQLPlanCommon"
]

from prettytable import PrettyTable

from models.mongoengine import *


class BaseOracleSQLPlanCommon:
    """基础oracle的sqlplan"""

    ORACLE_PLAN_HEAD = {
        'Id': "the_id",
        'Operation': "operation_display_with_options",
        'Name': "object_name",
        'Rows': "cardinality",
        'Bytes': "the_bytes",
        'Cost (%CPU)': "cost",
        'Time': "time"
    }

    @classmethod
    def sql_plan_table(cls, **kwargs) -> str:
        """
        输出SQLPlus风格的oracle执行计划表格
        :return:
        """
        raise NotImplementedError

    @classmethod
    def _format_sql_plan_table(cls, plans: mongoengine_qs) -> str:
        pt = PrettyTable(cls.ORACLE_PLAN_HEAD.keys())
        pt.align = "l"
        for plan in plans.values_list(*cls.ORACLE_PLAN_HEAD.values()):
            to_add = [i if i is not None else " " for i in plan]
            for i in range(len(to_add)):
                try:
                    to_add[i] = int(to_add[i])
                except ValueError:
                    try:
                        to_add[i] = float(to_add[i])
                    except Exception as e:
                        pass
            m, s = divmod(to_add[-1] if to_add[-1] and to_add[-1] != " " else 0, 60)
            h, m = divmod(m, 60)
            to_add[-1] = "%02d:%02d:%02d" % (h, m, s)
            # for i in range(3, 6):  # convert to int if range 3~5 is not None
            #     if to_add[i] and to_add[i] to_add[i].strip():
            #         to_add[i] = int(to_add[i])
            if 8 > len(str(to_add[3])) > 5:
                to_add[3] = str(round(to_add[3] // 1024)) + "K"
                if len(str(to_add[3])) >= 8:
                    to_add[3] = str(round(to_add[3] // 1024 // 1024)) + "M"
            if 8 > len(str(to_add[4])) > 5:
                to_add[4] = str(round(to_add[4] // 1024)) + "K"
                if len(str(to_add[4])) >= 8:
                    to_add[4] = str(round(to_add[4] // 1024 // 1024)) + "M"
            if 8 > len(str(to_add[5])) > 5:
                to_add[5] = str(round(to_add[5] // 1024)) + "K"
                if len(str(to_add[5])) >= 8:
                    to_add[5] = str(round(to_add[5] // 1024 // 1024)) + "M"
            pt.add_row(plan)
        return str(pt)
