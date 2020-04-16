# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "schema_no_data"
]

from contextlib import contextmanager


@contextmanager
def schema_no_data(label: str):
    """用于打印schema的相关数据的计数日志"""
    schema_no_data = set()

    def caller(schema: str, count: int):
        if count:
            print(f"{schema}-{label}: {count}")
        else:
            schema_no_data.add(schema)

    yield caller

    if schema_no_data:
        schema_no_data_str = ", ".join(schema_no_data)
        print(f"following schema(s) have no {label}:"
              f" {schema_no_data_str}")
