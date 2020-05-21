# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "BaseReq"
]

import json
from typing import *

from tornado.web import RequestHandler
from tornado.concurrent import Future
from schema import SchemaError, Or
from sqlalchemy import or_

from utils.schema_utils import *
from utils.datetime_utils import *
from models.sqlalchemy import sqlalchemy_q
from models.mongoengine import mongoengine_qs, Q
from . import exceptions


class BaseReq(RequestHandler):
    """base request handler"""

    @staticmethod
    def _resp_em(e):
        """return error message if a schema validation failed."""

        def s(*args, **kwargs):
            raise exceptions.SchemaErrorWithMessage(e)

        return s

    @classmethod
    def scm_or_with_error_msg(cls, *args, e):
        """按照Or的顺序执行schema，如果都失败了，则返回400，msg为e"""
        args = list(args) + [cls._resp_em(e)]
        return Or(*args)

    @classmethod
    def scm_one_of_choices(cls, choices, allow_empty=False):
        if not allow_empty:
            return cls.scm_or_with_error_msg(
                scm_one_of_choices(choices),
                e=f"should be one of {choices}"
            )
        else:
            return cls.scm_or_with_error_msg(
                scm_empty_as_optional(scm_one_of_choices(choices)),
                e=f"should be one of {choices}"
            )

    @classmethod
    def scm_subset_of_choices(cls, choices, allow_empty=False):
        if not allow_empty:
            return cls.scm_or_with_error_msg(
                scm_subset_of_choices(choices),
                e=f"should be subset of {choices}"
            )
        else:
            return cls.scm_or_with_error_msg(
                scm_empty_as_optional(scm_subset_of_choices(choices)),
                e=f"should be subset of {choices}"
            )

    def get_json_args(self,
                      schema_object: Schema = None,
                      default_body: str = None) -> Union[dict, list, None]:
        """
        获取非get请求情况下，http request body所带的json数据
        :param schema_object:
        :param default_body: 如果http request body没有数据，则用该数据替换，
                                如果该数据为None，就报错
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
             ) -> "Future[None]":
        content = dt_to_str(content)  # 直接把时间对象都转成文本
        resp_structure_base = {
            "msg": msg,  # 提示信息，多为错误信息
            "content": content,  # 返回实质内容
        }
        resp_structure_kwargs = {
            "total": None,  # （列表）返回的项目总数
            "pages": None,  # （列表）返回的总页数
            "page": None,  # （列表）当前返回的页数
            "per_page": None  # （列表）当前返回的每页项目数
        }
        if kwargs:
            resp_structure_kwargs.update(kwargs)
            resp_structure_base.update(resp_structure_kwargs)
        self.set_status(status_code)
        self.set_header("Content-Type", "application/json")
        self.set_header("Cache-control", "no-cache")
        return self.finish(json.dumps(resp_structure_base, ensure_ascii=False))

    def resp_created(self, *args, **kwargs) -> "Future[None]":
        kwargs["status_code"] = 201
        return self.resp(*args, **kwargs)

    def resp_bad_req(self, msg: str = "bad request") -> "Future[None]":
        return self.resp(msg=msg, status_code=400)

    def resp_forbidden(self, msg: str = "forbidden") -> "Future[None]":
        return self.resp(msg=msg, status_code=403)

    def resp_not_found(
            self,
            msg: str = "not found",
            content: Union[dict, list] = None, **kwargs) -> "Future[None]":
        return self.resp(msg=msg, content=content, status_code=404, **kwargs)

    def resp_unauthorized(self, msg: str = "未登录。") -> "Future[None]":
        return self.resp(msg=msg, status_code=401)

    def resp_bad_username_password(
            self, msg: str = "用户名或者密码错误。") -> "Future[None]":
        return self.resp(msg=msg, status_code=400)

    @staticmethod
    def paginate(query, page: int = 1, per_page: int = 10) -> (list, dict):
        """分页"""
        if isinstance(query, mongoengine_qs):
            items = query.skip((page - 1) * per_page).limit(per_page)
        elif isinstance(query, sqlalchemy_q):
            items = query.limit(per_page).offset((page - 1) * per_page).all()
        elif isinstance(query, (list, tuple)):
            items = query[(page - 1) * per_page:(page - 1) * per_page + per_page]
        else:
            assert 0
        if page == 1 and len(items) < per_page:
            total = len(items)
        elif isinstance(query, (list, tuple)):
            total = len(query)
        elif isinstance(query, sqlalchemy_q):
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
        """
        时间段过滤
        输入参数，为True表示必选，False表示必不选，None或者date对象表示可选并设置默认值
        :param date_start:
        :param date_end:
        :return:
        """
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
        return query_dict.pop("date_start", None), \
               query_dict.pop("date_end", None)

    @staticmethod
    def query_keyword(q, l, *args) -> Union[mongoengine_qs, sqlalchemy_q]:
        """
        查询sql语句的like，或者mongo的regex，以匹配类似项
        :param q: query object
        :param l: 查询的文本，不带查询符号
        :param args: sqlalchemy的字段
        :return: q
        """
        if isinstance(q, sqlalchemy_q):
            l = f"%{l}%"
            to_or = []
            for s in args:
                to_or.append(s.ilike(l))
            return q.filter(or_(*to_or))

        elif isinstance(q, mongoengine_qs):
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

    # @classmethod
    # def test_case(cls,
    #               method: Callable,
    #               headers=None,
    #               query_string=None,
    #               json_body=None,
    #               files=None) -> NoReturn:
    #     """
    #     生成测试参数
    #     :param method:
    #     :param headers: 请求方法的引用（cls.get, cls.post, ...）
    #     :param query_string:
    #     :param json_body:
    #     :param files: {"filename": <byte of the file>, ...}
    #     :return:
    #     """
    #     return
