# -*- coding: utf-8 -*-
import random
import string
from plain_db.mongo_operat import MongoHelper

class RuleUtils:

    rules = [x for x in MongoHelper.find("rule", {})]

    @classmethod
    def compare(cls, dict_a, dict_b):
        if len(dict_a.keys()) >= len(dict_b.keys()):
            dict_b, dict_a = dict_a, dict_b
        return all([dict_a[x] == dict_b[x] for x in dict_a])

    @classmethod
    def get_classified_rules(cls, rule_type, db_type, db_model, rule_status):
        subset = {
            'rule_type': rule_type,
            'db_type': db_type,
            'db_model': db_model,
            'rule_status': rule_status,
        }
        return {x['rule_name']: x for x in cls.rules if cls.compare(subset, x)}

    @classmethod
    def text(cls, db_model, db_type="O", rule_status="ON"):
        return cls.get_classified_rules("TEXT", db_type, db_model, rule_status)

    @classmethod
    def obj(cls, db_model, db_type="O", rule_status="ON"):
        return cls.get_classified_rules("OBJ", db_type, db_model, rule_status)

    @classmethod
    def sqlplan(cls, db_model, db_type="O", rule_status="ON"):
        return cls.get_classified_rules("SQLPLAN", db_type, db_model, rule_status)

    @classmethod
    def sqlstat(cls, db_model, db_type="O", rule_status="ON"):
        return cls.get_classified_rules("SQLSTAT", db_type, db_model, rule_status)

    @classmethod
    def rule_info(cls):
        return {rule['rule_name']: rule for rule in cls.rules}

    @classmethod
    def gen_random_collection(cls):
        """随机生成mongo中collection的名称"""
        tmp0 = "tmp" + ''.join(random.sample(string.ascii_lowercase, 3))
        tmp1 = "tmp" + ''.join(random.sample(string.ascii_lowercase, 3))
        return tmp0, tmp1

# obj = partial(RuleUtils.get_classified_rules, "OBJ")
# text = partial(RuleUtils.get_classified_rules, "TEXT")
# sqlplan = partial(RuleUtils.get_classified_rules, "SQLPLAN")
# sqlstat = partial(RuleUtils.get_classified_rules, "SQLSTAT")
