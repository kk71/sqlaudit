# -*- coding: utf-8 -*-

import pymongo
import settings


class MongoOperat(object):
    """mongodb连接模块，负责初始化mongo，认证，动态获取集合等功能"""

    def __init__(self, hostname, port, dbname=None, account=None, password=None):
        """mongo初始化"""
        self.conn = pymongo.MongoClient(hostname, port, serverSelectionTimeoutMS=2000, connect=False)
        # self.conn = pymongo.MongoClient(hostname, port)
        if account and password:
            self.conn.admin.authenticate(account, password)
        self.dbname = dbname if dbname else "sqlreview"
        self.db = getattr(self.conn, self.dbname)

    def get_collection(self, collection):
        """
        动态获取collection
        参数 collection：collecion名称
        示例：
            client = MongoOperat("127.0.0.1", 27017)
            rule = client.get_collection("rule")
        """
        return getattr(self.db, collection)

    def command(self, rule_cmd, nolock=True):
        """
        利用mongo自身的优势去执行一些命令
        参数 rule_cmd: 需要在mongo中执行的一些语句
            nolock: 防止阻塞选项
        示例：
            client = MongoOperat("127.0.0.1", 27017)
            rule_cmd = "db.rule.find({'db_type' : 'O'})"
            client.command(rule_cmd)
        """
        self.db.command("eval", rule_cmd, nolock)

    def find(self, collection, sql, condition=None, **kwargs):
        result = self.get_collection(collection).find(sql, condition, no_cursor_timeout=True, **kwargs).batch_size(5)
        return result

    def find_one(self, collection, sql, condition=None, **kwargs):
        result = self.get_collection(collection).find_one(sql, condition, **kwargs)
        return result

    def update(self, collection, sql, condition=None):
        self.get_collection(collection).update(sql, condition)
        # try:
        #     # print("On mongo update collection: %s. " % collection)
        #     self.get_collection(collection).update(sql, condition)
        # except Exception as e:
        #     print("Exception on mongo update")
        #     print(e)

    def update_one(self, collection, sql, condition=None):
        self.get_collection(collection).update_one(sql, condition)
        # try:
        #     # print("On mongo update one collection: %s. " % collection)
        #     self.get_collection(collection).update_one(sql, condition)
        # except Exception as e:
        #     print("Exception on mongo update one")
        #     print(e)

    def insert_one(self, collection, sql, condition=None):
        return self.get_collection(collection).insert_one(sql, condition)
        # try:
        #     # print("On mongo insert one collection: %s. " % collection)
        #     object_id = self.get_collection(collection).insert_one(sql, condition)
        #     return object_id
        # except Exception as e:
        #     print("Exception on insert one")
        #     print(e)

    def insert(self, collection, sql, condition=None):
        return self.get_collection(collection).insert(sql, condition)
        # try:
        #     # print("On mongo insert collection: %s. " % collection)
        #     object_id = self.get_collection(collection).insert(sql, condition)
        #     return object_id
        # except Exception as e:
        #     print(e)

    def drop(self, collection):
        self.get_collection(collection).drop()

    def aggregate(self, collection, sql):
        result = self.get_collection(collection).aggregate(sql)
        return result

    def distinct(self, collection, sql):
        result = self.get_collection(collection).distinct(sql)
        return result

MongoOb = MongoOperat
MongoDB = MongoOperat

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
        # try:
        #     # print("On mongo update collection: %s. " % collection)
        #     cls.get_collection(collection).update(sql, condition)
        # except Exception as e:
        #     print("Exception on mongo update")
        #     print(e)

    @classmethod
    def update_one(cls, collection, sql, condition=None):
        return cls.get_collection(collection).update_one(sql, condition)
        # try:
        #     # print("On mongo update one collection: %s. " % collection)
        #     return cls.get_collection(collection).update_one(sql, condition)
        # except Exception as e:
        #     print("Exception on mongo update one")
        #     print(e)

    @classmethod
    def insert_one(cls, collection, sql, condition=None):
        return cls.get_collection(collection).insert_one(sql, condition)
        # try:
        #     # print("On mongo insert one collection: %s. " % collection)
        #     object_id = cls.get_collection(collection).insert_one(sql, condition)
        #     return object_id
        # except Exception as e:
        #     print("Exception on insert one")
        #     print(e)

    @classmethod
    def insert(cls, collection, sql, condition=None):
        return cls.get_collection(collection).insert(sql, condition)
        # try:
        #     # print("On mongo insert collection: %s. " % collection)
        #     object_id = cls.get_collection(collection).insert(sql, condition)
        #     return object_id
        # except Exception as e:
        #     print(e)

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


if __name__ == "__main__":
    mongoc = MongoDB('114.115.135.118', 27017, 'sqlreview', account='sqlreview', password='sqlreview')

