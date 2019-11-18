# Author: kk.Fang(fkfkbill@gmail.com)

import json

from schema import Schema, Optional

import settings
from utils.schema_utils import *


# 版本文件结构校验
file_structure = Schema({
    "versions": [
        {
            "version": [scm_int],
            "version_name": scm_str,
            "client_name": scm_str,
            "msg": scm_unempty_str,
            "developers": [scm_unempty_str],
            "create_time": scm_datetime,
            Optional(object): object
        }
    ]
})


def get_versions(filename=settings.VERSION_FILE):
    """
    获取版本信息
    :param filename:
    :return:
    """
    with open(filename, "r") as z:
        original_file_content = z.read()
        structure = json.loads(original_file_content)
        # validation, if not then throw exception
        validated = file_structure.validate(structure)
    return validated


def add_version(filename=settings.VERSION_FILE):
    """
    增加版本
    :param filename:
    :return:
    """
