# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseReq",
    "AuthReq",
    "PrivilegeReq",
    "as_view"
]

import json
from typing import *

import jwt
from tornado.web import RequestHandler
from schema import SchemaError, Or
from mongoengine import QuerySet as M_Query
from mongoengine import Q
from sqlalchemy.orm.query import Query as S_Query
from sqlalchemy import or_

import settings
from utils.perf_utils import timing, r_cache
from utils.schema_utils import *
from utils.privilege_utils import *
from utils.const import AdminRequired
from utils.datetime_utils import *


class SchemaErrorWithMessageResponsed(Exception):
    pass


class TokenHasExpiredException(Exception):
    pass


@timing(cache=r_cache)
def paginate_count_using_cache(qs):
    """用于给分页器存储总数，因为总数查询有的时候也很耗时"""
    return qs.order_by(None).count()


class BaseReq(RequestHandler):
    """basic request handler"""

    def _resp_em(self, e):
        """return error message if a schema validation failed."""
        def s(*args, **kwargs):
            self.resp_bad_req(msg=e)
            raise SchemaErrorWithMessageResponsed(e)
        return s

    def scm_with_em(self, *args, e):
        """按照Or的顺序执行schema，如果都失败了，则返回400，msg为e"""
        args = list(args) + [self._resp_em(e)]
        return Or(*args)

    def get_json_args(self,
                      schema_object: Schema = None,
                      default_body: str = None) -> Union[dict, list, None]:
        """
        获取非get请求情况下，http request body所带的json数据
        :param schema_object:
        :param default_body: 如果http request body没有数据，则用该数据替换，如果该数据为None，就报错
        :return:
        """
        try:
            if not self.request.body and default_body is not None:
                ja = json.loads(default_body)
            else:
                ja = json.loads(self.request.body)
            if schema_object:
                ja = schema_object.validate(ja)
        except SchemaError as e:
            if not self._finished:
                self.resp_bad_req(msg=f"参数错误：{str(e)}")
                raise e
            return
        except json.JSONDecodeError:
            self.resp_bad_req(msg=f"参数错误：json解析失败。")
            return
        return ja

    def get_query_args(self, schema_object: Schema = None) -> Union[dict, None]:
        qa = {k: v[0].decode("utf-8")
              for k, v in self.request.query_arguments.items()}
        if schema_object:
            try:
                qa = schema_object.validate(qa)
            except SchemaError as e:
                if not self._finished:
                    self.resp_bad_req(msg=f"参数错误：{str(e)}")
                    raise e
                return
        return qa

    @staticmethod
    def alternative_args(
            func: Callable, schema_to_validate: Schema, k, **kwargs):
        """
        带条件的执行不同的schema检查
        :param func: self.get_query_args或者self.get_json_args
        :param schema_to_validate: 条件参数的schema
        :param k: 该参数的字段名
        :param kwargs: 枚举参数可能的值，并且给一个当前func的schema对象
                        (注意，匹配的k的值是schema验证过之后的)
        :return: (字段k的值（验证过的）, 对应验证后的params)
        """
        schema_to_validate._ignore_extra_keys = True
        k_params = func(schema_to_validate)
        k_value = k_params.pop(k)
        current_schema_to_validate: Schema = kwargs[k_value]
        current_schema_to_validate._ignore_extra_keys = True
        return k_value, func(current_schema_to_validate)

    def resp(self,
        content: Union[dict, list] = None,
        msg: str = "",
        status_code: int = 200,
        **kwargs
             ) -> NoReturn:
        resp_structure_base = {
            "msg": msg,                  # 提示信息，多为错误信息
            "content": content,          # 返回实质内容
        }
        resp_structure_kwargs = {
            "total": None,               # （列表）返回的项目总数
            "pages": None,               # （列表）返回的总页数
            "page": None,                # （列表）当前返回的页数
            "per_page": None             # （列表）当前返回的每页项目数
        }
        if kwargs:
            resp_structure_kwargs.update(kwargs)
            resp_structure_base.update(resp_structure_kwargs)
        self.set_status(status_code)
        self.set_header("Content-Type", "application/json")
        self.set_header("Cache-control", "no-cache")
        self.finish(json.dumps(resp_structure_base, ensure_ascii=False))

    def resp_created(self, *args, **kwargs) -> NoReturn:
        kwargs["status_code"] = 201
        self.resp(*args, **kwargs)

    def resp_bad_req(self, msg: str = "bad request") -> NoReturn:
        self.resp(msg=msg, status_code=400)

    def resp_forbidden(self, msg: str = "forbidden") -> NoReturn:
        self.resp(msg=msg, status_code=403)

    def resp_not_found(self, msg: str = "not found", content: Union[dict, list] = None, **kwargs) -> NoReturn:
        self.resp(msg=msg, content=content, status_code=404, **kwargs)

    def resp_unauthorized(self, msg: str = "未登录。") -> NoReturn:
        self.resp(msg=msg, status_code=401)

    def resp_bad_username_password(self, msg: str = "用户名或者密码错误。") -> NoReturn:
        self.resp(msg=msg, status_code=400)

    @staticmethod
    def paginate(query, page: int = 1, per_page: int = 10) -> (list, dict):
        """分页"""
        if isinstance(query, M_Query):
            items = query.skip((page - 1) * per_page).limit(per_page)
        elif isinstance(query, S_Query):
            items = query.limit(per_page).offset((page - 1) * per_page).all()
        elif isinstance(query, (list, tuple)):
            items = query[(page-1)*per_page:(page-1)*per_page+per_page]
        else:
            assert 0
        if page == 1 and len(items) < per_page:
            total = len(items)
        elif isinstance(query, (list, tuple)):
            total = len(query)
        elif isinstance(query, S_Query):
            total = query.order_by(None).count()
        else:
            # total = paginate_count_using_cache(qs=query)
            total = query.order_by(None).count()
        pages = total // per_page
        if total % per_page > 0:
            pages += 1
        return items, {"page": page, "per_page": per_page, "total": total, "pages": pages}

    @staticmethod
    def gen_p(page=1, per_page=10):
        """分页的配置"""
        return {
            scm_optional("page", default=page): scm_gt0_int,
            scm_optional("per_page", default=per_page): scm_gt0_int,
        }

    @staticmethod
    def pop_p(query_dict) -> dict:
        """弹出分页相关的两个字段"""
        return {"page": query_dict.pop("page"), "per_page": query_dict.pop("per_page")}

    def pop_p_for_paginating(self, query_dict) -> NoReturn:
        """弹出分页相关的两个字段，并且在操作分页的时候默认使用这个字段"""
        pass

    @staticmethod
    def gen_date(
            date_start: Union[None, bool, date] = None,
            date_end: Union[None, bool, date] = None):
        """时间段过滤"""
        ret = {}
        if date_start is True:
            ret["date_start"] = scm_date
        elif date_start is False:
            pass
        else:
            ret[scm_optional("date_start", default=date_start)] = scm_date
        if date_end is True:
            ret["date_end"] = scm_date_end
        elif date_end is False:
            pass
        else:
            ret[scm_optional("date_end", default=date_end)] = scm_date_end
        return ret

    @staticmethod
    def pop_date(query_dict) -> (object, object):
        """弹出时间起始和终止两个字段，如果任一个字段不存在那么就以None替代"""
        return query_dict.pop("date_start", None),\
               query_dict.pop("date_end", None)

    @staticmethod
    def query_keyword(q, l, *args) -> Union[M_Query, S_Query]:
        """
        查询sql语句的like，或者mongo的regex，以匹配类似项
        :param q: query object
        :param l: 查询的文本，不带查询符号
        :param args: sqlalchemy的字段
        :return: q
        """
        if isinstance(q, S_Query):
            l = f"%{l}%"
            to_or = []
            for s in args:
                to_or.append(s.ilike(l))
            return q.filter(or_(*to_or))

        elif isinstance(q, M_Query):
            to_query = Q()
            for s in args:
                to_query = to_query | Q(**{f"{s}__icontains": l})
            return q.filter(to_query)

        else:
            assert 0

    @classmethod
    def dict_to_verbose_dict_in_list(cls, d, key_name="key", value_name="value"):
        """将普通字典转成繁琐的list of dicts"""
        return [{key_name: k, value_name: v} for k, v in d.items()]

    @classmethod
    def list_of_dict_to_date_axis(cls,
                                  iterable,
                                  date_key_name,
                                  value_key_name,
                                  date_to_str=True) -> list:
        """
        [{}, ...]转换为折线图、柱状图所需要的格式(x轴为日期，y轴为某个值)
        会对date去重，按照时间由晚及早去重
        :param iterable:
        :param date_key_name: 日期（或者日期时间，会自动转换）所在的键名
        :param value_key_name: y轴的值所在的键名
        :param date_to_str: 是否将date对象转换为str
        :return:
        """
        date_set = set()
        deduplicate_list = []
        for pair in sorted(
                [[i[date_key_name], i[value_key_name]] for i in iterable], reverse=True):
            if isinstance(pair[0], date) and not isinstance(pair[0], datetime):
                # 是一个日期对象，但不是日期时间对象
                pass
            else:
                pair[0] = pair[0].date()
            if pair[0] in date_set:
                continue
            date_set.add(pair[0])
            if date_to_str:
                pair[0] = d_to_str(pair[0])
            deduplicate_list.append(pair)
        deduplicate_list.reverse()
        return deduplicate_list


class AuthReq(BaseReq):
    """a request handler with authenticating"""

    def __init__(self, *args, **kwargs):
        super(AuthReq, self).__init__(*args, **kwargs)

    def get_current_user(self) -> NoReturn:
        token = self.request.headers.get("token", None)
        try:
            if not token:
                raise Exception("No token is present.")
            payload = jwt.decode(token, key=settings.JWT_SECRET, algorithm=settings.JWT_ALGORITHM)
        except:
            self.current_user = None
            self.resp_unauthorized()
            return
        now_timestamp = arrow.now().timestamp
        try:
            data = Schema({
                "login_user": scm_unempty_str,
                "timestamp": object,
                scm_Optional(object): object  # 兼容未来可能增加的字段
            }).validate(payload)
            # 验证token的超时
            if now_timestamp - data["timestamp"] > settings.JWT_EXPIRE_SEC:
                raise TokenHasExpiredException()
            self.current_user: str = data["login_user"]
        except SchemaError:
            self.current_user = None
            self.resp_bad_req(msg="请求的token payload结构错误。")
            return
        except TokenHasExpiredException:
            self.current_user = None
            self.resp_unauthorized(msg="请重新登录。")
            return
        print(f'* {self.current_user} - {settings.JWT_EXPIRE_SEC - (now_timestamp - data["timestamp"])}s to expire - {token}')

    def prepare(self) -> Optional[Awaitable[None]]:
        self.get_current_user()

    def is_admin(self):
        return settings.ADMIN_LOGIN_USER == self.current_user

    def acquire_admin(self):
        """如果不是admin就报错"""
        if not self.is_admin():
            self.resp_forbidden(msg="仅限管理员操作。")
            raise AdminRequired


class PrivilegeReq(AuthReq):
    """a request handler with role & privilege check"""

    def __init__(self, *args, **kwargs):
        super(AuthReq, self).__init__(*args, **kwargs)

    def should_have(self, *args):
        """judge what privilege is not present for current user"""
        if self.is_admin():
            return set()  # 如果是admin用户，则认为任何权限都是拥有的
        privilege_list = get_privilege_towards_user(self.current_user)
        return set(args) - set(privilege_list)

    def has(self, *args):
        """judge if privilege is all present for current user"""
        if self.should_have(*args):
            return False
        return True

    def acquire(self, *args):
        """ask for privilege, if not, response forbidden"""
        unavailable_privileges = self.should_have(*args)
        if unavailable_privileges:
            unavailable_privileges_names = ", ".join([
                PRIVILEGE.privilege_to_dict(i)["name"] for i in unavailable_privileges])
            self.resp_forbidden(msg=f"权限不足：{unavailable_privileges_names}")
            raise PrivilegeRequired

    def current_roles(self) -> list:
        """returns role_ids to current user"""
        return list(get_role_of_user(self.current_user).get(self.current_user, set([])))


def as_view(route_rule: str):
    def as_view_inner(req_handler: BaseReq):
        import restful_api.urls
        restful_api.urls.dynamic_urls.append((route_rule, req_handler))
        return req_handler
    return as_view_inner
