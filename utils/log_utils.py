# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "grouped_count_logger"
]

from contextlib import contextmanager


@contextmanager
def grouped_count_logger(
        label: str,
        item_type_name: str,
        show_item_type_name: bool = False,
        show_label_in_print: bool = False):
    """
    分组计数打印日志

    举例：

    schema_a [<obj-a>, ...total=3]
    schema_b [<obj-a>, ...total=1]
    schema_c [total=0]
    :return:
    """
    item_name_no_data = set()

    def caller(item_name: str, count: int):
        assert isinstance(item_name, str)
        if count:
            representation = [item_name]
            if show_label_in_print:
                representation.append(label)
            if show_item_type_name:
                representation.insert(0, item_type_name)
            print(f"{'-'.join(representation)}: {count}")
        else:
            item_name_no_data.add(item_name)

    yield caller

    if item_name_no_data:
        item_name_no_data_str = ", ".join(item_name_no_data)
        print(f"following {item_type_name}(s) have no {label}:"
              f" {item_name_no_data_str}")
