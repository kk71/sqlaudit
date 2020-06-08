# Author: kk.Fang(fkfkbill@gmail.com)

from typing import List, Dict

from mongoengine import StringField

import ticket.const
import ticket.ticket


class OracleTicket(ticket.ticket.Ticket):
    """oracle工单"""

    schema_name = StringField()
    online_username = StringField(default="")
    online_password = StringField(default="")

    @classmethod
    def num_stats(cls, cmdb_ids: List[int]) -> Dict[str, int]:
        """统计各种状态的工单的个数"""
        ticket_stats = OracleTicket.aggregate(
            {
                "$match": {
                    "cmdb_id": {"$in": cmdb_ids}
                }
            },
            {
                "$group": {
                    "_id": {
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            }
        )
        ticket_stats_dict: Dict[str, int] = {
            i: 0
            for i in ticket.const.ALL_TICKET_STATUS_CHINESE.values()
        }
        for i in ticket_stats:
            chinese_status_name = ticket.const.ALL_TICKET_STATUS_CHINESE[
                i["_id"]["status"]]
            ticket_stats_dict[chinese_status_name] += i["count"]
        return ticket_stats_dict
