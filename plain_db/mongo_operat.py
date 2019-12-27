
__all__ = [
    "MongoHelper"
]

import pymongo
import settings


class MongoHelper:
    """mongodb连接模块，负责初始化mongo，认证，动态获取集合等功能"""

    conn = None
    db = None

    @classmethod
    def get_db(cls):

        if cls.db is None:

            cls.conn = pymongo.MongoClient(
                settings.MONGO_SERVER,
                settings.MONGO_PORT,
                connect=False,
                serverSelectionTimeoutMS=2000
            )

            cls.conn.admin.authenticate(settings.MONGO_USER, settings.MONGO_PASSWORD)
            cls.db = getattr(cls.conn, settings.MONGO_DB)

        return cls.db

    @classmethod
    def get_collection(cls, collection):
        """
        动态获取collection
        参数 collection：collecion名称
        示例：
            client = MongoOperat("127.0.0.1", 27017)
            rule = client.get_collection("rule")
        """
        return getattr(cls.get_db(), collection)

    @classmethod
    def command(cls, rule_cmd, nolock=True):
        """
        利用mongo自身的优势去执行一些命令
        参数 rule_cmd: 需要在mongo中执行的一些语句
            nolock: 防止阻塞选项
        示例：
            client = MongoOperat("127.0.0.1", 27017)
            rule_cmd = "db.rule.find({'db_type' : 'O'})"
            client.command(rule_cmd)
        """
        return cls.get_db().command("eval", rule_cmd, nolock)

    @classmethod
    def collection_names(cls):
        return cls.get_db().collection_names()

    @classmethod
    def find(cls, collection, sql, condition=None, **kwargs):
        result = cls.get_collection(collection).find(sql, condition, no_cursor_timeout=True, **kwargs).batch_size(5)
        return result

    @classmethod
    def find_one(self, collection, sql, condition=None, **kwargs):
        result = self.get_collection(collection).find_one(sql, condition, **kwargs)
        return result

    @classmethod
    def update(cls, collection, sql, condition=None):
        cls.get_collection(collection).update(sql, condition)

    @classmethod
    def update_one(cls, collection, sql, condition=None):
        return cls.get_collection(collection).update_one(sql, condition)

    @classmethod
    def insert_one(cls, collection, sql, condition=None):
        return cls.get_collection(collection).insert_one(sql, condition)

    @classmethod
    def insert(cls, collection, sql, condition=None):
        return cls.get_collection(collection).insert(sql, condition)

    @classmethod
    def drop(cls, collection):
        cls.get_collection(collection).drop()

    @classmethod
    def aggregate(cls, collection, sql):
        result = cls.get_collection(collection).aggregate(sql)
        return result

    @classmethod
    def distinct(cls, collection, sql):
        result = cls.get_collection(collection).distinct(sql)
        return result

    @classmethod
    def remove(cls, collection, condition=None):
        result = cls.get_collection(collection).remove(condition)
        return result
