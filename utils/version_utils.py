# Author: kk.Fang(fkfkbill@gmail.com)

import json
from copy import deepcopy

from schema import Schema, Optional

import settings
from utils.datetime_utils import *
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
    default_structure = {"versions": []}
    try:
        with open(filename, "r") as z:
            original_file_content = z.read()
            if original_file_content:
                structure = json.loads(original_file_content)
            else:
                structure = default_structure
            # validation, if not then throw exception
            validated = file_structure.validate(structure)
    except FileNotFoundError:
        return default_structure
    return validated


def add_version(filename=settings.VERSION_FILE):
    """
    增加版本
    :param filename:
    :return:
    """
    print("Now going to add new version...")
    new_v = {"create_time": dt_to_str(arrow.now())}
    old_versions = get_versions(filename)["versions"]
    last_version = old_versions[-1] if old_versions else {}

    # version
    new_version = new_v["version"] = None
    new_version_default = deepcopy(last_version.get("version", [0, 0, 0]))
    new_version_default[-1] += 1
    while not new_version:
        new_version = input(f"input the new version(default as {new_version_default}): ")
        if not new_version:
            new_version = new_version_default
        else:
            new_version = [int(i) for i in new_version.split(".") if i]
            if not new_version:
                print("at least input one number.")
                continue
            for old_version in old_versions:
                if new_version == old_version["version"]:
                    print("duplicated version!")
                    new_version = None
                    break
    new_v["version"] = new_version

    # version name
    new_version_name_default = last_version.get("version_name", None)
    new_version_name = new_v["version_name"] = None
    while not new_version_name:
        new_version_name = input("input the new version name"
                                 f"(default as '{new_version_name_default}'): ")
        if not new_version_name:
            new_version_name = new_version_name_default
        if not new_version_name:
            continue
    new_v["version_name"] = new_version_name

    # client name
    new_client_name_default = last_version.get("client_name", None)
    new_client_name = new_v["client_name"] = None
    while not new_client_name:
        new_client_name = input("input the new client name"
                                f"(default as {new_client_name_default}):")
        if not new_client_name:
            new_client_name = new_client_name_default
        if not new_client_name:
            continue
    new_v["client_name"] = new_client_name

    # msg
    new_msg = new_v["msg"] = None
    while not new_msg:
        new_msg = input("input msg to show what this version fixed and upgraded: ")
    new_v["msg"] = new_msg

    # developers
    new_developers = new_v["developers"] = None
    new_developers_default = last_version.get("developers", [])
    while not new_developers:
        new_developers = input(f"developers(default as {new_developers_default}): ")
        if not new_developers:
            new_developers = new_developers_default
            if not new_developers:
                continue
        else:
            new_developers = list(set([i.strip() for i in new_developers.split(",")
                                       if i.strip()]))
            if not new_developers:
                continue
    new_v["developers"] = new_developers

    with open(filename, "w") as z:
        old_versions.append(dt_to_str(new_v))
        z.write(json.dumps({"versions": dt_to_str(old_versions)}, indent=4, ensure_ascii=False))
    current_version = get_versions()["versions"][-1]
    print("updated.")
    return current_version
