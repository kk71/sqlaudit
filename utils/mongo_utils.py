# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "temp_collection"
]


import random
from contextlib import contextmanager


def get_random_collection_name(prefix):
    """产生一个随机的collection名称，用于aggregation"""
    seed = list("1234567890qazwsxedcrfvtgbyhnujmikolp")
    random.shuffle(seed)
    rad = "".join(seed[:9])
    return f"{prefix}-{rad}"


@contextmanager
def temp_collection(mongo_client, collection_prefix):
    """更安全的使用一个临时的collection"""
    collection_name = None
    try:
        collection_name = get_random_collection_name(collection_prefix)
        yield mongo_client.get_collection(collection_name)
        mongo_client.drop(collection_name)
    except Exception as e:
        print(f"exception occurs when operating temporary collection {collection_name}, going to drop it.")
        mongo_client.drop(collection_name)
        raise e
