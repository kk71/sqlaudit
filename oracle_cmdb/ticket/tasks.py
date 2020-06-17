# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "OracleTicketAnalyse"
]

from models import init_models

init_models()

import ticket.const
import ticket.exceptions
import rule.const
import task.const
from models.sqlalchemy import make_session
from .analyse import OracleSubTicketAnalyse
from .ticket import OracleTicket
from .sub_ticket import OracleSubTicket
from ticket.ticket import TempScriptStatement, TicketScript
from ticket.single_sql import SingleSQLForTicket
from cmdb.cmdb import CMDB
from rule.rule_jar import RuleJar
from cmdb.const import DB_ORACLE
from task.task import *


@register_task(task.const.TASK_TYPE_ORACLE_TICKET_ANALYSE)
class OracleTicketAnalyse(BaseTask):

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        """
        诊断线下工单
        """
        ticket_id = kwargs["ticket_id"]
        script_ids = kwargs["script_ids"]
        the_ticket = OracleTicket.filter(ticket_id=ticket_id).first()
        if not the_ticket:
            raise ticket.exceptions.TicketNotFound(
                "fatal: the ticket with ticket_id={ticket_id} is not found.")
        if the_ticket.status != ticket.const.TICKET_ANALYSING:
            raise ticket.exceptions.TicketWithWrongStatus(
                "fatal: current ticket has wrong status.")

        sub_tickets = []
        scripts: {str: TicketScript} = dict()
        static_rules = RuleJar.gen_jar_with_entries(
            rule.const.RULE_ENTRY_TICKET_STATIC,
            cmdb_id=the_ticket.cmdb_id
        )
        dynamic_rules = RuleJar.gen_jar_with_entries(
            rule.const.RULE_ENTRY_TICKET_DYNAMIC,
            cmdb_id=the_ticket.cmdb_id
        )
        with make_session() as session:
            cmdb = session.query(CMDB).filter_by(cmdb_id=the_ticket.cmdb_id).first()
            for the_script_id in script_ids:
                statement_q = TempScriptStatement.filter(
                    script__script_id=the_script_id).order_by("position")
                sqls = [
                    # {single-sql}: 格式化成通用的sql结构
                    SingleSQLForTicket.gen_from_temp_script(ts) for ts in statement_q
                ]
                for the_statement in statement_q:
                    single_sql = SingleSQLForTicket.gen_from_temp_script(the_statement)
                    osta = OracleSubTicketAnalyse(
                        static_rules=static_rules,
                        dynamic_rules=dynamic_rules,
                        cmdb=cmdb,
                        ticket=the_ticket
                    )
                    sub_ticket: OracleSubTicket = osta.run(
                        single_sql=single_sql,
                        sqls=sqls
                    )
                    sub_ticket.script = the_statement.script
                    sub_tickets.append(sub_ticket)
                    if the_script_id not in scripts.keys():
                        scripts[the_script_id] = sub_ticket.script
        the_ticket.status = ticket.const.TICKET_PENDING
        the_ticket.scripts = list(scripts.values())
        if sub_tickets:
            print(f"finally we got {len(sub_tickets)} sub tickets.")
            OracleSubTicket.insert(sub_tickets)
            the_ticket.calculate_score()
            # 计算脚本下面的子工单数，以及整个工单的子工单数
            for the_script in the_ticket.scripts:
                the_script.update_sub_ticket_count_from_sub_ticket()
            the_ticket.update_sub_ticket_count_from_scripts()
            the_ticket.save()
        else:
            raise ticket.exceptions.NoSubTicketGenerated
        print(f"{the_ticket} is successfully analysed.")
