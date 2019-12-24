# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from models.oracle import *
from models.mongo import *
from task.base import *
from utils.const import *


@celery.task
def offline_ticket(work_list_id: int, session_id: str):
    """
    诊断线下工单
    """
    with make_session() as session:
        ticket = session.query(WorkList).\
            filter(WorkList.work_list_id == work_list_id).first()
        if not ticket:
            raise Exception("fatal: the ticket with "
                            f"work_list_id={work_list_id} is not found.")
        elif ticket.work_list_status != OFFLINE_TICKET_ANALYSING:
            raise Exception("fatal: current ticket has wrong status.")
        cmdb = session.query(CMDB).filter(CMDB.cmdb_id == ticket.cmdb_id).first()
        if not cmdb:
            raise CMDBNotFoundException(ticket.cmdb_id)
        sqls = session.query(
                WorkListAnalyseTemp.sql_text,
                WorkListAnalyseTemp.comments,
                WorkListAnalyseTemp.num).\
            filter(WorkListAnalyseTemp.session_id == session_id).\
            order_by(WorkListAnalyseTemp.num.desc())
        if not sqls:
            raise Exception("fatal: no SQL was given in session_id.")
        print(f"* going to analyse {len(sqls)} sqls "
              f"in ticket({ticket.task_name}, {ticket.work_list_id}), "
              f"cmdb_id({ticket.cmdb_id}), schema({ticket.schema_name})")
        sub_tickets = []
        sub_ticket_analysis = SubTicketAnalysis()
        for sql, comment, num in sqls:
            sub_ticket = sub_ticket_analysis.run(
                session, cmdb, ticket.schema_name, sql, num)
            sub_tickets.append(sub_ticket)
        ticket.calc_score()
        ticket.work_list_status = OFFLINE_TICKET_PENDING
        TicketSubResult.objects.insert(sub_tickets)
        session.add(ticket)
