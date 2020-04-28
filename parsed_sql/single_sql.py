# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "SingleSQL"
]


class SingleSQL(dict):
    """传给规则使用的单条sql语句的信息"""

    def __init__(self,
                 sql_text: str,
                 sql_text_no_comment: str,
                 comments: str,
                 position: int,
                 sql_type: str,
                 **kwargs):
        kwargs.update(
            sql_text=sql_text,
            sql_text_no_comment=sql_text_no_comment,
            comments=comments,
            position=position,
            sql_type=sql_type,
        )
        super().__init__(**kwargs)
