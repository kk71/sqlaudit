# Author: kk.Fang(fkfkbill@gmail.com)


class AuthException(Exception):
    """认证，权限方面的异常"""
    pass


class TokenExpiredException(AuthException):
    """token过期异常"""
    pass


# mail发送时间
ALL_SEND_DATE = ("星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日")
ALL_SEND_TIME = ("0:00", "1:00", "2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00",
                 "9:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00",
                 "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00")

