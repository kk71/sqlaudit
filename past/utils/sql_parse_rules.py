# -*- coding: utf-8 -*-

import re


class ObjStaticRules:

    @classmethod
    def check_sequnce(cls, sql, db_model):

        if not re.search(r"create\s+sequence", sql, re.I):
            return True, None

        if 'order' in sql:
            return False, "使用了order的序列"
        if ' cache ' not in sql:
            return False, "没有显式指定cache并且序列的cache需要指定为2000或以上"
        res = re.search(r"cache\s+(\d+)", sql, re.I)
        if res and int(res.group(1)) < 2000:
            return False, "序列的cache需要指定为2000或以上"

        return True, None

    @classmethod
    def check_concurrency_leve(cls, sql, db_model):

        if not re.search(r"create\s+table", sql, re.I):
            return True, None

        if 'parallel' not in sql:
            return True, None

        res = re.search("parallel\s+(\d)", sql, re.I)
        if res and int(res.group(1)) > 1:
            return False, "表，索引不能设置并行度"

        return True, None

    @classmethod
    def check_lob_using(cls, sql, db_model):

        if not re.search(r"create\s+table", sql, re.I) and not re.search(r"alter\s+table", sql, re.I):
            return True, None

        if any([x in sql.lower() for x in ['blob', 'clob', 'bfile', 'xmltype']]):
            return False, "高频表上不推荐使用LOB字段"

        return True, None

    @classmethod
    def check_column_number(cls, sql, db_model):

        if not re.search(r"create\s+table", sql, re.I):
            return True, None

        left_brackets = 0
        left_flag = 0
        right_flag = 0
        for index, s in enumerate(sql):
            if s == "(":
                left_brackets += 1
                if left_brackets == 1:
                    left_flag = index
            elif s == ")":
                left_brackets -= 1
                if left_brackets == 0:
                    right_flag = index
                    break

        sql = sql[left_flag + 1: right_flag]

        if len(sql.split(',')) > 255:
            return False, "表字段个数不能超过255"
        return True, None

    @classmethod
    def check_bitmap_index(cls, sql, db_model):

        if db_model == "OLTP" and re.search("create\s+bitmap\s+index", sql, re.I):
            return False, "不建议创建位图索引"
        return True, None

    @classmethod
    def check_dblink_created(cls, sql, db_model):

        if re.search('create\s+database\s+link', sql, re.I):
            return False, "不建议创建DB LINK"
        return True, None

    @classmethod
    def check_index_not_define_table_space(cls, sql, db_model):

        if re.search('create\s+index', sql, re.I) and 'tablespace' not in sql:
            return False, "需要为索引指定表空间"
        return True, None

    # @classmethod
    # def check_alter_table_not_support(cls, sql, db_model):

    #     if re.match(r'\s*alter\s+table', sql, re.I) and re.search(r'add.*default', sql, re.I):

    #         group_dict = re.match(r'\s*alter\s+table\s+(?P<table_name>\w+)', sql, re.I).groupdict()

    #         table_name = group_dict.get('table_name', 'table_name')
    #         return False, f"修改意见：alter table {table_name} add NEW_COLUMN number NOT NULL\nalter table {table_name} modify NEW_COLUMN default DEFAULT_VALUE;"
    #     return True, None

    @classmethod
    def run(cls, sql, db_model):

        methods = [x for x in dir(ObjStaticRules) if x.startswith("check_")]
        errs = [getattr(cls, method)(sql.lower().strip(), db_model) for method in methods]
        return ';'.join([x[1] for x in errs if x[0] is False])


if __name__ == "__main__":

    pass
