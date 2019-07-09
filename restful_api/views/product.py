# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema

from utils.schema_utils import *
from .base import BaseReq
from past.utils.product_license import *


class ProductLicenseHandler(BaseReq):

    def get(self):
        """查询序列号状态"""
        DEFAULT_DISPLAY_LENGTH = 128
        try:
            license_key = SqlAuditLicenseKeyManager.latest_license_key()
            license_key_ins = SqlAuditLicenseKey.decode(license_key)
            if not license_key_ins.is_valid():
                raise DecryptError("license info is invalid")
            self.resp({
                'enterprise_name': license_key_ins.enterprise_name,
                'database_counts': license_key_ins.database_counts,
                'license_status': license_key_ins.license_status,
                'license_code': license_key[:DEFAULT_DISPLAY_LENGTH]
                if len(license_key) > DEFAULT_DISPLAY_LENGTH else license_key,
            })
        except DecryptError as e:
            print("license expired or invalid")
            return self.resp_bad_req(msg=str(e))
        except Exception as e:
            print(str(e))
            return self.resp_bad_req(msg=str(e))

    def post(self):
        """更新序列号"""
        param = self.get_json_args(Schema({
            "license": scm_unempty_str
        }))
        license_text = param.pop("license")
        try:
            if not SqlAuditLicenseKey.decode(license_text).is_valid():
                print(f"receive_license {license_text}")
                raise DecryptError("the license code is invalid")
            SqlAuditLicenseKeyManager(license_text,
                                      SqlAuditLicenseKeyManager.DB_LICENSE_STATUS["valid"],
                                      "get from website").insert()
            return self.resp_created(msg="")
        except DecryptError as e:
            msg = str(e)
            print(msg)
            return self.resp_bad_req(msg=f"解密错误--{msg}")
        except Exception as e:
            msg = str(e)
            print(msg)
            return self.resp_bad_req(msg=str(e))
