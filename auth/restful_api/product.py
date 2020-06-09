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
                    return self.resp_bad_req(msg="not license")
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

    get.argument = {
        "querystring": {},
        "json": {}
    }
    post.argument = {
        "querystring": {},
        "json": {
            "license": "CO4Yvhbu8CWPqsJPGbXqbQ51ZSU+tlrJaSzKXDXM6IoCyGsiXRIMO/WqGmGP2EBKO1ebjBq95YcbbVKSJeE+geR8FLTDZedDVJIrJ1rmTC49b3Yf8oCipBWxx512LKCj3FPSkR/kRYCADtDAp6UuujHcxa8Wxv+4bW8ZCg+bKGVgjjAI1dZ+qm7Y1JCAqh0+cWpDDq/WNz0Z+J8BAcSt/ifDtkuLDaX0GUHk5UqAGzdlHkAyYzRMpxcXsNLFOcwrexU9vvQg+x9Qlnd4O9R17qtFpZPCJ1ah3r0O+eyBftVTvRFQHCM8zqSw3txGd8RTbmg1gVyjaCuiZfDyrSnA9ffu+MmW52/lASgXP4MV+a8gVAAqauVkx0nxL2B42lhoQHYH48vlqr3vvRWV28Cphs2mNn8Xv3rvaCwUyoqX5gpsOwfP8SYQuZawA4MPAj4jouQ9dzv3Nun+vNLOeVie6oslNgFaX3wCSJD5vp9L1NKsh048AwC5OWK1qhvJsDk1qgXQ4Yyzd5iktcqW55owlRMpxiZu6XgDTq9/LZDz84lo/e2yPh4tz+f93jnOGI8mNMrznBj7F1JSq0qVHx5wWd/k/x/OxLaY4+Fs9rKsBk3axCE5+esQU9SJW+vMDSvbY+FUd/wMpdtNqMpFsEuAtNyhjm3n27j/rEhHsUOIHSRgWbzURhbSKrmaDMiVPCZlpGJ4RMxC/CylpJdp1KKR4l3gIjxxGHmn4P70HzGTgUZ1Irh4Cf3fTu5pXfnHsWgJCnP8wc5paDOntoLEKLes/nSAAsqOpL+IYwt1Dxdl+cn/tUoqqdLXNrusYZ4ct37VPPshqGl2TPUydAVwwcC2N5mG8fMKfmGl9mFmMe6DX2+cIZI9sWPcaKQotqsP7S6G8js+kvgQlPHlEwOt7xQN1sqDqQB8LM+4YqC/FTws5ZanTgvgFQZ2/1wfPfo9PAWYmFnI9rV5UKchn59XxolHR0B8l21nFX+w6YzDZKOs5RlZWYVBb6ivkrSGbgAlOorRJU4LujHEt+U04c3KXWS+YiaQl4h/DqKM8EBsbcIUr6R9/cp9clVuVzhyYqtkEPybAPs6qMhzCS7EDr6ZSjag/O2gI+nBGagQ2vBVrUmvF4ONoLYSkmUWC9kumYTswSFOZmdyH6mhY8PWjYJUMpUv4EtdkFYmwzj+nIn1vpXnUqQcZsjI5Peh/+SJML1CiA0QXar290rffB0G7U75c7jRQr8PrMULTQlfI8ZVYgF0iCSvRgj1Yc5eiv10s6xMHVSx84KUsS3x+Vh+9tw3PIZ5C1HFpcdh0aC+Eh68Py1gs6X1suCBs8KR/fpUkyqmkPcX4t33O2shZtzzNwwjSVdV/g==",
        }
    }

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

    get.argument = {
        "querystring": {
            "page": "1",
            "per_page": "10"
        },
        "json": {}
    }