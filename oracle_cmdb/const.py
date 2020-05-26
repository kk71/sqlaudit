# Author: kk.Fang(fkfkbill@gmail.com)


# 采集的sql数据是哪天的

ALL_TWO_DAYS_CAPTURE = (
    SQL_TWO_DAYS_CAPTURE_TODAY := "today",  # 当日至采集当时的
    SQL_TWO_DAYS_CAPTURE_YESTERDAY := "yesterday"  # 昨日0点至今日0点的
)

# 线上风险SQL规则OBJECT规则ENTRY
ONLINE_RISK_RULE_ENTRIES = (
    RULE_ENTRY_ONLINE_OBJECT := "OBJECT",
    RULE_ENTRY_ONLINE_SQL := "SQL"
)
