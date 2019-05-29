import time


from past.capture.sql import (
    OWNER_LIST_SQL,
    # OBJ_BASE_IDX_HEAP_INFO_SQL,
    # OBJ_BASE_IDX_HEAP_PHY_SIZE_SQL,
    OBJ_BASE_IDX_COL_HEAP_COL_INFO_SQL,
    # OBJ_PART_IDX_DETAIL_INFO_SQL,
    # OBJ_PART_IDX_INFO_SQL,
    # OBJ_PART_IDX_PHY_SIZE_SQL,
    OBJ_BASE_TAB_HEAP_INFO_SQL,
    # OBJ_PART_TAB_INFO_SQL,
    OBJ_PART_TAB_PARENT_PHY_SIZE_SQL,
    OBJ_PART_TAB_PARENT_COL_SQL,
    OBJ_PART_TAB_PARENT_SQL,
    # OBJ_PART_TAB_PHY_SIZE_SQL,
    # OBJ_PART_TAB_SON_PHY_SIZE_SQL,
    # OBJ_PART_TAB_SON_INFO_SQL,
    OBJ_TAB_COL_SQL,
    OBJ_VIEW_INFO_SQL
)
import past.capture.base


class CaptureObj(past.capture.base.Capture):
    """
    抓取obj
    """

    def __init__(self, mongo_client, db_cursor, etl_date, ipaddress, sid, record_id, cmdb_id):
        self.etl_date = etl_date
        self.ipaddress = ipaddress
        self.sid = sid
        self.record_id = record_id
        self.cmdb_id = cmdb_id
        super(CaptureObj, self).__init__(mongo_client, db_cursor)

    def extract_column(self, data, column=0):
        return [value[column] for value in data]

    def parse_result(self, data, columns, *args):
        full_dict = {}
        for value in data:
            temp_dict = {}
            for i in range(len(value)):
                temp_dict.update({
                    columns[i]: value[i],
                    'ETL_DATE': self.etl_date,
                    'IPADDR': self.ipaddress,
                    'SID': self.sid,
                    'cmdb_id': self.cmdb_id,
                    'record_id': self.record_id
                })
            index_list = [str(value[index]) for index in args]
            full_dict.update({"#".join(index_list): temp_dict})
        return full_dict

    # def obj_ind_info(self, obj_owner):
    #     records, columns = self.query_sql(
    #         OBJ_BASE_IDX_HEAP_INFO_SQL.format(obj_owner=obj_owner))
    #     phy_size, _ = self.query_sql(
    #         OBJ_BASE_IDX_HEAP_PHY_SIZE_SQL.format(obj_owner=obj_owner))
    #     phy_name = self.extract_column(phy_size)
    #     info_name = self.extract_column(records, column=1)
    #     phy_null = list(set(info_name) - set(phy_name))
    #     results = self.parse_result(records, columns, *(1,))
    #     for data in phy_null:
    #         results[data].update({"PHY_SIZE(MB)": 0})

    #     for item in phy_size:
    #         if results.get(item[1], None):
    #             results[item[1]].update({"PHY_SIZE(MB)": item[2]})
    #     return results

    def obj_ind_col_info(self, obj_owner):
        records, columns = self.query_sql(
            OBJ_BASE_IDX_COL_HEAP_COL_INFO_SQL.format(obj_owner=obj_owner))
        results = self.parse_result(records, columns, *(1, 4))
        return results

    # def obj_part_idx_detail(self, obj_owner):
    #    records, columns = self.query_sql(
    #        OBJ_PART_IDX_DETAIL_INFO_SQL.format(obj_owner=obj_owner))
    #    results = self.parse_result(records, columns, *(1, 4))
    #    return results

    def obj_tab_info(self, obj_owner):
        records, columns = self.query_sql(
            OBJ_BASE_TAB_HEAP_INFO_SQL.format(obj_owner=obj_owner))
        phy_size, _ = self.query_sql(
            OBJ_PART_TAB_PARENT_PHY_SIZE_SQL.format(obj_owner=obj_owner))
        col_num, _ = self.query_sql(
            OBJ_PART_TAB_PARENT_COL_SQL.format(obj_owner=obj_owner))
        phy_name = self.extract_column(phy_size)
        info_name = self.extract_column(records, column=1)
        phy_null = list(set(info_name) - set(phy_name))

        results = self.parse_result(records, columns, *(1,))
        for data in phy_null:
            if data not in results:
                continue
            results[data].update({"PHY_SIZE(MB)": 0})
        for item in phy_size:
            if item[0] not in results:
                continue
            results[item[0]].update({"PHY_SIZE(MB)": item[1]})

        for item in col_num:
            if item[0] not in results:
                continue
            results[item[0]].update({
                "COL_NUM": item[1]
            })
        return results

    # def obj_part_idx_info(self, obj_owner):
    #     records, columns = self.query_sql(
    #         OBJ_PART_IDX_INFO_SQL.format(obj_owner=obj_owner))
    #     phy_size, _ = self.query_sql(
    #         OBJ_PART_IDX_PHY_SIZE_SQL.format(obj_owner=obj_owner))

    #     results = self.parse_result(records, columns, *(0,))

    #     for item in phy_size:
    #         results[item[0]].update({
    #             "PHY_SIZE(MB)": item[1]
    #         })
    #     return results

    # def obj_part_tab(self, obj_owner):
    #     records, columns = self.query_sql(
    #         OBJ_PART_TAB_INFO_SQL.format(obj_owner=obj_owner))
    #     phy_size, _ = self.query_sql(
    #         OBJ_PART_TAB_PHY_SIZE_SQL.format(obj_owner=obj_owner))
    #     results = self.parse_result(records, columns, *(1, 6))
    #     key_list = results.keys()
    #     temp_dict = {"#".join([value[0], value[1]]): value[2] for value in phy_size}
    #     update_zero = list(set(key_list) ^ set(temp_dict.keys()))
    #     for value in update_zero:
    #         if value not in results:
    #             continue
    #         results[value].update({"PHY_SIZE(MB)": 0})
    #     for item in phy_size:
    #         if '#'.join([item[0], item[1]]) not in results:
    #             continue
    #         results["#".join([item[0], item[1]])].update(
    #             {"PHY_SIZE(MB)": item[2]})
    #     return results

    def obj_part_tab_parent(self, obj_owner):
        records, columns = self.query_sql(
            OBJ_PART_TAB_PARENT_SQL.format(obj_owner=obj_owner))
        phy_size, _ = self.query_sql(
            OBJ_PART_TAB_PARENT_PHY_SIZE_SQL.format(obj_owner=obj_owner))
        col_sum, _ = self.query_sql(
            OBJ_PART_TAB_PARENT_COL_SQL.format(obj_owner=obj_owner))
        results = self.parse_result(records, columns, *(1,))
        for item in phy_size:
            if item[0] not in results:
                results[item[0]]["PHY_SIZE(MB)"] = 0
            else:
                results[item[0]].update({"PHY_SIZE(MB)": item[1]})
        for val in col_sum:
            if val[0] not in results:
                results[val[0]]["NUM_ROW"] = 0
            else:
                results[val[0]].update({"NUM_ROW": val[1]})

        return results

    # def obj_part_tab_son(self, obj_owner):
    #    records, columns = self.query_sql(
    #        OBJ_PART_TAB_SON_INFO_SQL.format(obj_owner=obj_owner))
    #    phy_size, _ = self.query_sql(
    #        OBJ_PART_TAB_SON_PHY_SIZE_SQL.format(obj_owner=obj_owner))
    #    results = self.parse_result(records, columns, *(1, 6, 7))
    #    key_list = results.keys()
    #    temp_dict = {}
    #    [temp_dict.update({"#".join([value[0], value[1], value[2]]): value[3]})
    #        for value in phy_size]
    #    update_zero = list(set(key_list) ^ set(temp_dict.keys()))
    #    for value in update_zero:
    #        if value not in results:
    #            continue
    #        results[value].update({
    #            "PHY_SIZE(MB)": 0
    #        })
    #    for item in phy_size:
    #        if '#'.join([item[0], item[1], item[2]]) not in results:
    #            continue
    #        results["#".join([item[0], item[1], item[2]])].update({
    #            "PHY_SIZE(MB)": item[3]
    #        })
    #    return results

    def obj_tab_col(self, obj_owner):
        records, columns = self.query_sql(
            OBJ_TAB_COL_SQL.format(obj_owner=obj_owner))
        results = self.parse_result(records, columns, *(1, 2))
        return results

    def obj_view_info(self, obj_owner):
        records, columns = self.query_sql(
            OBJ_VIEW_INFO_SQL.format(obj_owner=obj_owner))
        results = self.parse_result(records, columns, *(1, 2))
        return results

    def run(self):
        owner_list, _ = self.query_sql(OWNER_LIST_SQL)
        methods = [x for x in dir(self) if "obj" in x]
        for obj_owner in owner_list:
            start = int(time.time() * 1000)
            for method in methods:
                results = getattr(self, method)(obj_owner[0])
                for name, info in results.items():
                    info.update({'cmdb_id': self.cmdb_id})
                    self.mongo_client.insert(method, info)
            print(f"{obj_owner} -> {int(time.time() * 1000) - start}ms")
