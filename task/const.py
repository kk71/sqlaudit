# Author: kk.Fang(fkfkbill@gmail.com)


# 任务种类

ALL_TASK_TYPES = (
    TASK_TYPE_CAPTURE := "CAPTURE"
)
ALL_TASK_TYPE_CHINESE = {
    TASK_TYPE_CAPTURE: "采集分析"
}


# 任务执行状态

ALL_TASK_EXECUTION_STATUS = (
    TASK_NEVER_RAN := 1,
    TASK_PENDING := 2,
    TASK_RUNNING := 3,
    TASK_DONE := 4,
    TASK_FAILED := 5
)
ALL_TASK_EXECUTION_STATUS_CHINESE_MAPPING = {
    TASK_NEVER_RAN: "从未执行",
    TASK_PENDING: "等待中",
    TASK_RUNNING: "正在执行",
    TASK_DONE: "成功",
    TASK_FAILED: "失败"
}

