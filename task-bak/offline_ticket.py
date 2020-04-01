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

        qe = QueryEntity(
            WorkListAnalyseTemp.sql_text,
            WorkListAnalyseTemp.sql_text_no_comment,
            WorkListAnalyseTemp.comments,
            WorkListAnalyseTemp.num,
            WorkListAnalyseTemp.sql_type
        )
        sqls = session.query(*qe).\
            filter(WorkListAnalyseTemp.session_id == session_id).\
            order_by(WorkListAnalyseTemp.num.desc())
        sqls: [dict] = [qe.to_dict(a_sql) for a_sql in sqls]
        if not sqls:
            raise Exception("fatal: no SQL was given in session_id.")
        print(f"* going to analyse {len(sqls)} sqls "
              f"in ticket({ticket.task_name}, {ticket.work_list_id}), "
              f"cmdb_id({ticket.cmdb_id}), schema({ticket.schema_name})")
        sub_tickets = []
        for single_sql in sqls:
            if cmdb.database_type in (DB_ORACLE, 1):
                sub_ticket_analysis = OracleSubTicketAnalysis(
                    cmdb=cmdb,
                    ticket=ticket
                )
            else:
                assert 0  # TODO add mysql support here
            sub_ticket = sub_ticket_analysis.run(
                sqls=sqls,
                single_sql=single_sql
            )
            sub_tickets.append(sub_ticket)
        ticket.work_list_status = OFFLINE_TICKET_PENDING

        if cmdb.database_type in (DB_ORACLE, 1):
            _TicketSubResult = OracleTicketSubResult
        else:
            assert 0  # TODO add mysql support here
        print(f"finally we got {len(sub_tickets)} sub_tickets.")
        _TicketSubResult.objects.insert(sub_tickets)
        ticket.calc_score()
        session.add(ticket)
    print(f"ticket with work_list_id = {work_list_id} is successfully analysed.")
