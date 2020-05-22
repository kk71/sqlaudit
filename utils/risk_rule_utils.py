
import json

from models.mongo.rule import Rule
from models.oracle import RiskSQLRule,make_session,Role


def export_risk_rule_to_json_file(filename: str):
    """导出risk rule to json file"""

    with make_session() as session:
        risk_rules=[i.to_dict() for i in session.query(RiskSQLRule).all()]
        with open(filename,'w') as z:
            z.write(json.dumps(risk_rules, indent=4, ensure_ascii=False))
        return len(risk_rules)

def import_from_risk_rule_json_file(filename: str):
    """
    从json文件导风险规则到oracle
    :param filename:
    :return: 导入数
    """
    with open(filename,"r") as z:
        risk_rules = json.load(z)
    with make_session() as session:
        old_risk_rule=session.query(RiskSQLRule)
        if old_risk_rule:
            old_risk_rule.delete()
        risks_rules=[]
        for x in risk_rules:
            r_r=RiskSQLRule(**x)
            risks_rules.append(r_r)
        session.add_all(risks_rules)
        session.commit()

        risk_rules_adjust=session.query(RiskSQLRule)
        rules_adjust=Rule.objects()
        rules_return=[]
        for x in rules_adjust:
            for y in risk_rules_adjust:
                if x.rule_name == y.rule_name:
                    if x in rules_return:
                        continue
                    if y.severity=="严重":
                        x.weight=30
                        x.max_score=30
                    if y.severity=="告警":
                        x.weight=25
                        x.max_score=50
                    if y.severity=="提示":
                        x.weight=0.1
                        x.max_score=5
                    rules_return.append(x)
        if rules_return:
            Rule.objects().delete()
            Rule.objects().insert(rules_return)
        return len(risks_rules),len(rules_return)
