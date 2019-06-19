# Author: kk.Fang(fkfkbill@gmail.com)

import json
from typing import *

import jwt
import arrow
from tornado.web import RequestHandler
from schema import Schema, SchemaError, Or
from schema import Optional as scm_Optional
from mongoengine import QuerySet as M_Query
from mongoengine import Q
from sqlalchemy.orm.query import Query as S_Query
from sqlalchemy import or_

import settings
from utils.schema_utils import scm_unempty_str, scm_int


__all__ = [
    "BaseReq",
    "AuthReq"
]


class SchemaErrorWithMessageResponsed(Exception):
    pass


class TokenHasExpiredException(Exception):
    pass


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
        qa = {k: v[0].decode("utf-8") for k, v in self.request.query_arguments.items()}
        if schema_object:
            try:
                qa = schema_object.validate(qa)
            except SchemaError as e:
                if not self._finished:
                    self.resp_bad_req(msg=f"参数错误：{str(e)}")
                    raise e
                return
        return qa

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

    def resp_unauthorized(self, msg: str = "please login before any operations") -> NoReturn:
        self.resp(msg=msg, status_code=401)

    def resp_bad_username_password(self, msg: str = "bad username or password") -> NoReturn:
        self.resp(msg=msg, status_code=403)

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
        else:
            total = query.order_by(None).count()  # this is so bad
        pages = total // per_page
        if total % per_page > 0:
            pages += 1
        return items, {"page": page, "per_page": per_page, "total": total, "pages": pages}

    @staticmethod
    def gen_p(page=1, per_page=10):
        """分页的配置"""
        return {
            scm_Optional("page", default=page): scm_int,
            scm_Optional("per_page", default=per_page): scm_int,
        }

    @staticmethod
    def pop_p(query_dict) -> dict:
        """弹出分页相关的两个字段"""
        return {"page": query_dict.pop("page"), "per_page": query_dict.pop("per_page")}

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
                to_or.append(s.like(l))
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
            self.resp_unauthorized(msg="token已过期，请重新登录。")
            return
        print(f'* {self.current_user} - {settings.JWT_EXPIRE_SEC - (now_timestamp - data["timestamp"])}s to expire - {token}')

    def prepare(self) -> Optional[Awaitable[None]]:
        self.get_current_user()
