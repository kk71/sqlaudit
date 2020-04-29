# Author: kk.Fang(fkfkbill@gmail.com)


# 数据库类型

ALL_DB_TYPE = (
    DB_ORACLE := "oracle",
    DB_MYSQL := "mysql"
)


# 业务类型

ALL_DB_MODEL = (
    MODEL_OLTP := "OLTP",
    MODEL_OLAP := "OLAP"
)


# 纳管库任务的操作来源

CMDB_TASK_OPERATOR_SCHEDULE = "SCHEDULE"
CMDB_TASK_OPERATOR_PERIODICAL = "PERIODICAL"
CMDB_TASK_OPERATOR_CLI = "CLI"

CMDB_TASK_OPERATOR_CHINESE = lambda x: {
        # TODO 注意！操作来源还一种情况是login_user，这个记录页面上的操作人，没有对应的中文
        CMDB_TASK_OPERATOR_SCHEDULE: "定时任务",
        CMDB_TASK_OPERATOR_PERIODICAL: "周期任务",
        CMDB_TASK_OPERATOR_CLI: "命令行"
    }.get(x, x)

