# Author: kk.Fang(fkfkbill@gmail.com)

from models import init_models
init_models()

from models.oracle import *
from models.mongo import *
from task.base import *
from utils.const import *
from utils.offline_utils import *


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

        if cmdb.database_type == DB_ORACLE:
            _TicketSubResult = OracleTicketSubResult
            _SubTicketAnalysis = OracleSubTicketAnalysis
        else:
            assert 0  # TODO add mysql support here

        qe = QueryEntity(
            WorkListAnalyseTemp.sql_text,
            WorkListAnalyseTemp.comments,
            WorkListAnalyseTemp.num,
            WorkListAnalyseTemp.sql_type
        )
        sqls = session.query(qe).\
            filter(WorkListAnalyseTemp.session_id == session_id).\
            order_by(WorkListAnalyseTemp.num.desc())
        if not sqls:
            raise Exception("fatal: no SQL was given in session_id.")
        print(f"* going to analyse {len(sqls)} sqls "
              f"in ticket({ticket.task_name}, {ticket.work_list_id}), "
              f"cmdb_id({ticket.cmdb_id}), schema({ticket.schema_name})")
        sqls: [dict] = qe.to_dict(sqls)
        sub_tickets = []
        sub_ticket_analysis = _SubTicketAnalysis(
            cmdb=cmdb,
            ticket=ticket
        ) if cmdb.database_type == DB_ORACLE else None  # TODO add mysql support here
        for single_sql in sqls:
            sub_ticket = sub_ticket_analysis.run(
                sqls=sqls,
                single_sql=single_sql
            )
            sub_tickets.append(sub_ticket)
        ticket.work_list_status = OFFLINE_TICKET_PENDING
        _TicketSubResult.objects.insert(sub_tickets)
        ticket.calc_score()
        session.add(ticket)
