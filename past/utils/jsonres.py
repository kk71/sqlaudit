# -*-coding:utf-8-*-

from past.utils.raiseerr import APIError


def temRes(func):
    def wrapper(parameter):
        try:
            response = func(parameter)
            if not isinstance(response, dict):
                raise APIError("类型错误", 10000)
            if 'message' not in response:
                response.update({'message': ''})
            if 'errcode' not in response:
                response.update({'errcode': 0})
        except APIError as e:
            response = {'message': e.message, 'errcode': e.errcode}
        parameter.write(response)
        return
    return wrapper
