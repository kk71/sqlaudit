from models.sqlalchemy import *
from restful_api import *
from utils.schema_utils import *
from utils.datetime_utils import *
from utils.version_utils import get_versions
from ..product_license import *
from ..license import License


@as_view("license", group="product")
class ProductLicenseHandler(BaseReq):

    def get(self):
        """查询产品纳管套数、可用天数、过期时间"""
        try:
            with make_session() as session:
                license = session.query(License).order_by(License.create_time.desc()).first()
                if not license:
                    self.resp(msg="not license")
                license_key = license.license_key
                license_key_ins = SqlAuditLicenseKey.decode(license_key)
                if not license_key_ins.is_valid():
                    raise DecryptError("license info is invalid")
                now = arrow.now()
                expire = arrow.get(license_key_ins.expired_day, [
                    "YYYY-M-DD HH:mm:ss",
                    "YYYY-MM-DD HH:mm:ss",
                    "YYYY-MM-D HH:mm:ss",
                    "YYYY-M-D HH:mm:ss",
                ]).shift(days=1)
                available_days = (expire - now).days
                self.resp({
                    'enterprise_name': license_key_ins.enterprise_name,
                    'available_days': available_days,
                    'expired_day': license_key_ins.expired_day,
                    'database_counts': license_key_ins.database_counts,
                    'license_status': license_key_ins.license_status,
                    'license_code': license_key,
                })
        except DecryptError as e:
            print("license expired or invalid")
            return self.resp_bad_req(msg=str(e))
        except Exception as e:
            print(str(e))
            return self.resp_bad_req(msg=str(e))

    def post(self):
        """更新lincen_key"""
        param = self.get_json_args(Schema({
            "license": scm_unempty_str
        }))
        license_text = param.pop("license")
        try:
            if not SqlAuditLicenseKey.decode(license_text).is_valid():
                print(f"receive_license {license_text}")
                raise DecryptError("the license code is invalid")
            with make_session() as session:
                license = License(license_key=license_text, license_status=1)
                session.add(license)
                session.commit()
            return self.resp_created(msg="")
        except DecryptError as e:
            msg = str(e)
            print(msg)
            return self.resp_bad_req(msg=f"解密错误--{msg}")
        except Exception as e:
            msg = str(e)
            print(msg)
            return self.resp_bad_req(msg=str(e))


@as_view("version", group="version")
class VersionHandler(BaseReq):

    def get(self):
        """获取版本信息"""
        params = self.get_query_args(Schema({
            **self.gen_p()
        }))
        p = self.pop_p(params)
        del params
        rst = get_versions()
        rst["versions"].reverse()
        rst["versions"], p = self.paginate(rst["versions"], **p)
        self.resp(dt_to_str(rst), **p)
