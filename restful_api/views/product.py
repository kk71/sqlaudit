# Author: kk.Fang(fkfkbill@gmail.com)

from schema import Schema, Optional, And, Or

import settings
from utils.schema_utils import *
from .base import BaseReq
from past.utils.product_license import *


class ProductLicenseHandler(BaseReq):
    name = "ProductLicenseHandler>"
    CALLBACK_TIME = 24 * 60 * 60 * 1000

    def __init__(self, *args, **kwargs):
        super(ProductLicenseHandler, self).__init__(*args, **kwargs)

    def get(self):
        """查询序列号状态"""
        DEFAULT_DISPLAY_LENGTH = 128
        license_key = ""
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
        receive_license = license_info = ""
        param = self.get_json_args(Schema({
            "license": scm_unempty_str
        }))
        try:
            if not SqlAuditLicenseKey.decode(receive_license).is_valid():
                print(f"receive_license {receive_license}")
                raise DecryptError(f"{flag} the license code is invalid")
            SqlAuditLicenseKeyManager(receive_license,
                                      SqlAuditLicenseKeyManager.DB_LICENSE_STATUS["valid"],
                                      "get from website").insert()
            self.resp_body["detail"] = "license code is valid and save successfully"

        except DecryptError as e:
            logger.error(f"{flag} error: ", exc_info=1)
            self.status_code = self.STATUS_CODE_REQUEST_ERROR
            self.status_desc = DecryptError.__name__
        except Exception as e:
            logger.warning(f"{flag} receive_license {receive_license}")
            logger.warning(f"{flag} license_info {license_info}")
            logger.error(f"{flag} error: ", exc_info=1)
            self.status_code = self.STATUS_CODE_SERVER_ERROR
            self.status_desc = ServiceError.__name__
        return self.resp()
