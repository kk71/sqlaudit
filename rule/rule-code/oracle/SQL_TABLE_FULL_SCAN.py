from oracle_cmdb.capture.obj_tab_info import ObjTabInfo


def code(rule, entries, **kwargs):
    sql_plan_qs = kwargs["sql_plan_qs"]
    schema_name: str = kwargs["schema_name"]
    task_record_id: int = kwargs["task_record_id"]

    plans = sql_plan_qs.filter(operation="TABLE ACCESS", options="FULL")

    for plan in plans:
        the_table = ObjTabInfo.objects(
            task_record_id=task_record_id,
            schema_name=schema_name,
            table_name=plan.object_name
        ).first()
        if the_table.num_rows >= rule.gip("table_row_num") or\
                the_table.phy_size_mb >= rule.gip("table_phy_size"):
            yield {}


code_hole.append(code)
