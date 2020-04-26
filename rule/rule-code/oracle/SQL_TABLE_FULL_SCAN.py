from oracle_cmdb.capture.obj_tab_info import OracleObjTabInfo
from rule.code_utils import *


def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]

    qs = sql_plan_qs.filter(
        operation="TABLE ACCESS",
        options="FULL",
        object_type="TABLE"
    )

    for d in values_dict(qs,
                         "sql_id",
                         "plan_hash_value",
                         "object_name",
                         "object_type",
                         "task_record_id",
                         "schema_name"):
        the_table = OracleObjTabInfo.objects(
            task_record_id=d["task_record_id"],
            schema_name=d["schema_name"],
            table_name=d["object_name"]
        ).first()
        if not the_table:
            continue
        if the_table.num_rows >= rule.gip("table_row_num") or\
                the_table.phy_size_mb >= rule.gip("table_phy_size"):
            yield {
                k: v
                for k, v in d.items()
                if k in ("sql_id", "plan_hash_value", "object_name", "object_type")
            }


code_hole.append(code)
