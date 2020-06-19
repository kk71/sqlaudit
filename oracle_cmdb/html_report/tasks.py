import tarfile
import os.path

import settings
import task.const
from task.task import *
from .utils import *


@register_task(task.const.TASK_TYPE_CMDB_REPORT)
class CmdbReportExportHtml(BaseTask):
    """CMDB库的html报告导出"""

    @classmethod
    def report(cls, path_prefix, filename, **kwargs):
        parame_dict: dict = kwargs["parame_dict"]

        v_page = print_html_script('SQL审核管控平台CMDB报告')
        cmdb = parame_dict["cmdb"]
        cmdb_print_html_body(v_page, cmdb)
        print_html_js(v_page)
        tablespace_sum = parame_dict["cmdb_overview"]["tablespace_sum"]
        # cmdb基本信息
        print_html_cmdb_basic_info(v_page, cmdb, tablespace_sum)

        # SQL质量
        sql_num = parame_dict["cmdb_overview"]["sql_num"]
        time_week = [arrow.get(x['date']).strftime('%Y-%m-%d') for x in sql_num["week"]['active']]
        active_week_data = [x['value'] for x in sql_num["week"]['active']]
        at_risk_week_data = [x['value'] for x in sql_num["week"]['at_risk']]
        time_mouth = [arrow.get(x['date']).strftime('%Y-%m-%d') for x in sql_num["month"]['active']]
        active_mouth_data = [x['value'] for x in sql_num["month"]['active']]
        at_risk_mouth_data = [x['value'] for x in sql_num["month"]['at_risk']]
        print_html_cmdb_sql_quality(v_page, time_week,
                                    active_week_data,
                                    at_risk_week_data,
                                    time_mouth,
                                    active_mouth_data,
                                    at_risk_mouth_data)

        # print_html_cmdb_radar(v_page, radar_avg, radar_score_avg,
        #                       radar_min, radar_score_min)
        # 表空间使用率排名
        tabspace_q = parame_dict["tabspace_q"]
        ts_usage_ratio = [x.usage_ratio for x in tabspace_q][:10]
        ts_name = [x.tablespace_name for x in tabspace_q][:10]
        sort_ts_usage_ration = sorted(ts_usage_ratio, reverse=False)
        print_html_cmdb_tab_space_use_ration(v_page, ts_name, ts_usage_ratio, sort_ts_usage_ration)

        # sql语句总耗时
        sql_execution_time_total = parame_dict["cmdb_overview"]["sql_execution_cost_rank"]["elapsed_time_total"]
        x = [x['time'] for x in sql_execution_time_total][:10]
        x.reverse()
        y = [y['sql_id'] for y in sql_execution_time_total][:10]
        y.reverse()
        print_html_cmdb_sql_time_num(v_page, x, y)

        # 风险规则排名
        risk_rule_rank = parame_dict["cmdb_overview"]["risk_rule_rank"]
        risk_name = [x["rule"]['desc'] for x in risk_rule_rank][:10]
        num = [y['issue_num'] for y in risk_rule_rank][:10]
        reverse_num = [y['issue_num'] for y in risk_rule_rank][:10]
        reverse_num.reverse()
        print_html_cmdb_risk_rule_rank(v_page, risk_name, num, reverse_num)

        # cmdb用户schema分数排名
        rank_schema_score = parame_dict["cmdb_overview"]["rank_schema_score"]
        x_schema = [x['schema_name'] for x in rank_schema_score]
        y_score = [y['score'] for y in rank_schema_score]
        print_html_cmdb_user_ranking(v_page, x_schema, y_score)

        # sql详情
        sqls = parame_dict["sql_detail"]
        print_html_cmdb_sql_details(v_page, sqls)

        print_html_cmdb_js(v_page)
        v_page.printOut(f"oracle_cmdb/html_report/sqlreviewcmdb.html")

        path = os.path.join(path_prefix, filename)
        tar = tarfile.open(str(path), "w:gz")
        tar.add("oracle_cmdb/html_report/css")
        tar.add("oracle_cmdb/html_report/assets")
        tar.add("oracle_cmdb/html_report/js")
        tar.add("oracle_cmdb/html_report/sqlreviewcmdb.html")
        tar.close()

        path = os.path.join(path_prefix, filename)
        return path

    @classmethod
    def task(cls, task_record_id: int, **kwargs):
        filename: str = kwargs.pop("filename")

        path = cls.report(settings.HEALTH_DIR, filename, **kwargs)
        return path