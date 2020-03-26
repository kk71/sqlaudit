# Author: kk.Fang(fkfkbill@gmail.com)

__all__ = [
    "ticket_analyse"
]

from models import init_models
init_models()

import ticket.const
import ticket.exceptions
from task.base import *
from models.oracle import make_session, CMDB
from .analyse import OracleSubTicketAnalyse
from .ticket import OracleTicket
from .sub_ticket import OracleSubTicket
from ticket.ticket import TempScriptStatement, TicketScript


def format_single_sql(ts):
    return {
        "sql_text": ts.normalized,
        "sql_text_no_comment": ts.normalized_without_comment,
        "comments": ts.comment,
        "position": ts.position,
        "sql_type": ts.sql_type
    }


@celery.task
def ticket_analyse(ticket_id: str, script_ids: [str]):
    """
    诊断线下工单
    """
    the_ticket = OracleTicket.objects(ticket_id=ticket_id).first()
    if not the_ticket:
        raise ticket.exceptions.TicketNotFound(
            "fatal: the ticket with ticket_id={ticket_id} is not found.")
    if the_ticket.status != ticket.const.TICKET_ANALYSING:
        raise ticket.exceptions.TicketWithWrongStatus(
            "fatal: current ticket has wrong status.")

    sub_tickets = []
    scripts: {str: TicketScript} = dict()
    with make_session() as session:
        cmdb = session.query(CMDB).filter_by(cmdb_id=the_ticket.cmdb_id).first()
        for the_script_id in script_ids:
            statement_q = TempScriptStatement.objects(
                script__script_id=the_script_id).order_by("position")
            sqls = [
                # {single-sql}: 格式化成通用的sql结构
                format_single_sql(ts) for ts in statement_q
            ]
            for the_statement in statement_q:
                single_sql = format_single_sql(the_statement)
                osta = OracleSubTicketAnalyse(
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
        OracleSubTicket.objects.insert(sub_tickets)
        the_ticket.calculate_score()
        # 计算脚本下面的子工单数，以及整个工单的子工单数
        for the_script in the_ticket.scripts:
            the_script.update_sub_ticket_count_from_sub_ticket()
        the_ticket.update_sub_ticket_count_from_scripts()
        the_ticket.save()
    else:
        raise ticket.exceptions.NoSubTicketGenerated
    print(f"{ticket} is successfully analysed.")
