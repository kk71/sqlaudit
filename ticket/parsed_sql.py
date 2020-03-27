# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ParsedSQL",
    "ParsedSQLStatement"
]

import re
import pickle

import sqlparse

from . import const


class ParsedSQL(list):
    """存储一个格式化分解之后的sql脚本"""

    def __init__(self, sql: str):
        """
        把一整个sql脚本分解为一个个的语句，并且作一些处理
        :param sql: 原始sql脚本
        """
        self._original_sql: str = sql
        # sql里面的remark是不能被sqlparse当作注释处理的，需要先替换掉
        # 需要在后面把这个备注换回去
        tmpl_replace_remark = re.compile(r"^\s*remark", re.I | re.M)
        sql_remark_replaced: str = tmpl_replace_remark.sub(
            const.REMARK_PLACEHOLDER, sql)
        # 去掉注释的sql只是暂存，后续可能有用
        self._comment_striped_sql: str = sqlparse.format(
            sql_remark_replaced, strip_comments=True)
        parsed_sqlparse_sql_statements = sqlparse.parse(sql_remark_replaced)
        super(ParsedSQL, self).__init__([ParsedSQLStatement(i)
                                         for i in parsed_sqlparse_sql_statements
                                         if i.normalized.strip()])

    def get_original_sql(self):
        return self._original_sql

    def get_comment_striped_sql(self):
        return self._comment_striped_sql

    def filter_by_sql_type(self, sql_type: int) -> ["ParsedSQLStatement"]:
        """按照sql类型（DML，DDL）返回想要的Statement对象list"""
        return [i for i in self if i.sql_type == sql_type]


class ParsedSQLStatement:
    """简单重写的sqlparse.sql.Statement，用以支持更多的语句检测"""

    def __init__(self, sss: sqlparse.sql.Statement):

        # 处理过的语句
        # 这里把之前替换掉的remark替换回去
        tmpl_replaced_remark = re.compile(
            rf"^\s*{const.REMARK_PLACEHOLDER}", re.I | re.M)
        self.normalized = tmpl_replaced_remark.sub(
            "remark", sss.normalized).strip()

        # sql里面的remark是不能被sqlparse当作注释处理的，
        # 需要先替换成普通的注释，再用sqlparse去掉注释
        tmpl_replace_remark = re.compile(r"^\s*remark", re.I | re.M)
        sql_remark_replaced: str = tmpl_replace_remark.sub(
            const.REMARK_PLACEHOLDER, self.normalized)
        # 去掉注释的sql只是暂存，后续可能有用
        self.normalized_without_comment: str = sqlparse.format(
            sql_remark_replaced, strip_comment=True)

        # 语句内的组成部分
        self.tokens = sss.tokens

        # 语句的关键字名（select, alter，...）
        self.statement_type = sss.get_type()
        if self.statement_type == "UNKNOWN":
            token_num = 0
            while self.statement_type not in const.ALL_SQL_KEYWORDS and \
                    self.tokens:
                ft = self.tokens[token_num]
                self.statement_type = ft.normalized
                token_num += 1
                if token_num > len(self.tokens) - 1:
                    break
        if self.statement_type not in const.ALL_SQL_KEYWORDS:
            print(f"warning: statement type {self.statement_type} not in "
                  "predefined keywords list.")

        # 当前语句的类型（DDL，DML还是别的）
        if self.statement_type in const.SQL_KEYWORDS[const.SQL_DDL]:
            self.sql_type = const.SQL_DDL
        elif self.statement_type in const.SQL_KEYWORDS[const.SQL_DML]:
            self.sql_type = const.SQL_DML
        else:
            self.sql_type = "UNKNOWN"
            print(f"warning: statement '{self.normalized}' cannot "
                  "be recognized whether DDL or DML.")

    def __repr__(self):
        sql_thumbnail = self.normalized_without_comment[:20]
        if sql_thumbnail != self.normalized_without_comment:
            sql_thumbnail += "..."
        return "<ParsedSQLStatement-" \
               f"{const.ALL_SQL_TYPE_NAME_MAPPING[self.sql_type]}-" \
               f"{self.statement_type}-" \
               f"{sql_thumbnail}>"

    def serialize(self):
        return {
            "normalized": self.normalized,
            "normalized_without_comment": self.normalized_without_comment,
            "statement_type": self.statement_type,
            "sql_type": self.sql_type
        }
