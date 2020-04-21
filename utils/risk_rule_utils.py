
import json

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
        for x in risk_rules:
            r_r=RiskSQLRule(**x)
            session.add(r_r)
        session.commit()
        return len(risk_rules)