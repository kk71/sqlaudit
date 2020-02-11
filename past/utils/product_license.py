# -*- coding: utf-8 -*-
# pycryptodomex==3.7.3

import json
import time
import base64
import os.path
from functools import wraps

from Cryptodome.PublicKey import RSA
from Cryptodome.Cipher import PKCS1_OAEP

import settings
from plain_db.oracleob import OracleHelper, DBError


def count_time(start):
    s = time.time() - start
    print(s)


def generate_keys():
    start = time.time()
    modulus_length = 8192
    key = RSA.generate(modulus_length)
    pub_key = key.publickey()
    count_time(start)
    return key, pub_key


def _encrypt_msg(a_message, public_key):
    encryptor = PKCS1_OAEP.new(public_key)
    encrypted_msg = encryptor.encrypt(a_message)
    encoded_encrypted_msg = base64.b64encode(encrypted_msg)
    return encoded_encrypted_msg.decode("utf8")


def _decrypt_msg(encoded_encrypted_msg, private_key):
    encryptor = PKCS1_OAEP.new(private_key)
    decoded_encrypted_msg = base64.b64decode(encoded_encrypted_msg)
    decoded_decrypted_msg = encryptor.decrypt(decoded_encrypted_msg)
    return decoded_decrypted_msg.decode("utf8")


def export_keys():
    private, public = generate_keys()
    print((private.exportKey()).decode("utf8"))
    print((public.exportKey()).decode("utf8"))


def read_key_file(path):
    with open(path) as f:
        key = f.read()
        return RSA.importKey(key)


def generate_raw_msg(enterprise_name, unique_key, license_status,
                     install_date, available_days, expired_day, database_counts):
    msg = {
        "enterprise_name": enterprise_name,
        "unique_key": unique_key,
        "license_status": license_status,
        "install_date": install_date,
        "available_days": available_days,
        "expired_day": expired_day,
        "database_counts": database_counts,
    }
    return json.dumps(msg)


def encrypt_msg(raw, public_key_file=settings.PRIVATE_KEY):
    public_key = read_key_file(public_key_file)
    return _encrypt_msg(raw.encode('utf8'), public_key)


def decrypt_msg(encoded, private_key_file=settings.PRIVATE_KEY):
    try:
        private_key = read_key_file(private_key_file)
        decoded = _decrypt_msg(encoded, private_key)
        plaintext = json.loads(decoded)
    except Exception as e:
        raise DecryptError(e.__str__())
    return plaintext


class DecryptError(Exception):
    pass


class EncryptError(Exception):
    pass


class InvalidLicenseKey(Exception):
    pass


class SqlAuditLicenseKey:
    INVALID_LICENSE = -1
    SERVER_ERROR = 1
    VALID_LICENSE = 0
    LICENSE_STATUS = {"available": 0, "forever": 1}
    public_key = settings.PUBLIC_KEY
    private_key = settings.PRIVATE_KEY

    def __init__(self, enterprise_name="", unique_key="", install_date="",
                 available_days=0, database_counts=0, expired_day="", license_status=0):
        """

        :param enterprise_name: 公司名称
        :param unique_key: 主板编号
        :param install_date: 安装日期
        :param available_days: 可用天数
        :param database_counts: 纳管数据库
        :param expired_day: 过期日期
        :param license_status: 证书状态，0 启用；1 永久
        """
        self.enterprise_name = enterprise_name
        self.unique_key = unique_key
        self.install_date = install_date
        self.available_days = available_days
        self.database_counts = database_counts
        self.expired_day = expired_day
        self.license_status = license_status
        self._encoded_json = None

    @property
    def key_json_style(self):
        if self._encoded_json is None:
            self._encoded_json = generate_raw_msg(
                self.enterprise_name,
                self.unique_key,
                self.license_status,
                self.install_date,
                self.available_days,
                self.expired_day,
                self.database_counts
            )
        return self._encoded_json

    def generate(self):
        return encrypt_msg(self.key_json_style, self.public_key)

    @classmethod
    def decode(cls, license_key):
        try:
            decoded = decrypt_msg(license_key, private_key_file=cls.private_key)
            return SqlAuditLicenseKey(enterprise_name=decoded.get("enterprise_name"),
                                      unique_key=decoded.get('unique_key'),
                                      install_date=decoded.get('install_date'),
                                      available_days=decoded.get('available_days'),
                                      database_counts=decoded.get('database_counts'),
                                      expired_day=decoded.get('expired_day'),
                                      license_status=decoded.get('license_status'))
        except Exception as e:
            print("[SqlAuditLicenseKey].decode", e)
            raise DecryptError(e)

    def is_valid(self):
        # print("self is", self.license_status, self.available_days, )
        try:
            if int(self.license_status) == self.LICENSE_STATUS["forever"]:
                return True
            if int(self.license_status) == self.LICENSE_STATUS["available"] \
                    and int(self.available_days) > 0:
                return True
            else:
                return False
        except Exception as e:
            return False


class SqlAuditLicenseKeyManager:
    DB_LICENSE_STATUS = {
        "valid": 1,
        "invalid": 0,
    }

    def __init__(self, license_key, license_status, comment):
        """

        :param license_key: 注册码
        :param license_status: 注册码 0 异常；1 正常
        :param comment: ERROR_MSG
        """
        self.license_key = license_key
        self.license_status = license_status
        self.license_comment = comment

    def insert(self):
        sql = """INSERT INTO ISQLAUDIT.T_LICENSE_MANAGE
                        (LICENSE_KEY, LICENSE_STATUS, ERROR_MSG)
                        VALUES (:1, :2, :3)"""
        params_list = [self.license_key, self.license_status, self.license_comment]
        err = OracleHelper.insert(sql, params=params_list)
        if err:
            raise DBError(f"insert license to db failed, {err}")

    @staticmethod
    def latest_license_key():
        sql = """SELECT * FROM (
        SELECT * FROM T_LICENSE_MANAGE ORDER BY UPDATE_TIME DESC
        ) WHERE rownum=1"""
        res = OracleHelper.select_dict(sql, one=True)
        return res.get("license_key")


def available_license(method):
    """
    if latest license code is expired or decode error, invalid
    if latest license code not found, invalid
    else valid
    :return:
    """

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        name = "<decorator-available_license> works"
        url = self.get_login_url()
        try:
            latest = SqlAuditLicenseKeyManager.latest_license_key()
            if not SqlAuditLicenseKey.decode(latest).is_valid():
                print(f"{name}, the license code is invalid")
                if self.request.method == "GET":
                    self.render("login.html")
                    return
                self.redirect(url)
                return
        except Exception as e:
            if self.request.method == "GET":
                self.render("login.html")
                return
            print("<decorator-available_license> meet error", e)
            self.redirect(url)
            return
        return method(self, *args, **kwargs)

    return wrapper


def main(raw, private_key, public_key):
    message = raw.encode('utf8')
    print(message)
    encoded = _encrypt_msg(message, public_key)
    print(encoded)
    print("encode msg length", len(encoded))
    decoded = _decrypt_msg(encoded, private_key)
    print("decode msg: ", json.loads(decoded))


def gen_license():
    """创建新的license"""
    enterprise_name = "咪咕"
    unique_key = "BFEBFBFF00000F27;" * 8
    license_status = "1"
    install_date = "2020-01-01 00:00:00"
    available_days = 365*3
    expired_day = "2025-12-31 00:00:00"
    database_counts = 20

    raw_msg = generate_raw_msg(enterprise_name, unique_key, license_status,
                               install_date, available_days, expired_day, database_counts)
    print("raw_msg", len(raw_msg))
    print(encrypt_msg(raw_msg))
