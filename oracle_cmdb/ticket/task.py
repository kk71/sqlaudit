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
from ticket.ticket import TempScriptStatement


@celery.task
def ticket_analyse(ticket_id: str, script_id: [str]):
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
    with make_session() as session:
        cmdb = session.query(CMDB).filter_by(cmdb_id=the_ticket.cmdb_id).first()
        for the_script in the_ticket.scripts:
            statement_q = TempScriptStatement.objects(
                script__script_id=the_script.script_id).order_by("position")
            sqls = [
                # 格式化成通用的sql结构
                {
                    "sql_text": ts.normalized,
                    "sql_text_no_comment": ts.normalized_without_comment,
                    "comments": ts.comment,
                    "position": ts.position,
                    "sql_type": ts.sql_type
                } for ts in statement_q
            ]
            for the_sql in sqls:
                osta = OracleSubTicketAnalyse(
                    cmdb=cmdb,
                    ticket=the_ticket
                )
                sub_ticket = osta.run(
                    single_sql=the_sql,
                    sqls=sqls
                )
                sub_tickets.append(sub_ticket)
    the_ticket.status = ticket.const.TICKET_PENDING
    if sub_tickets:
        print(f"finally we got {len(sub_tickets)} sub tickets.")
        OracleSubTicket.objects.insert(sub_tickets)
        the_ticket.calculate_score()
    else:
        raise ticket.exceptions.NoSubTicketGenerated
    print(f"{ticket} is successfully analysed.")
