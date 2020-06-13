import arrow
import os
import tarfile
import settings

from .utils import *


def cmdb_report_export_html(cmdb,cmdb_q,tablespace_sum,
                            cmdb_score,
                            radar_avg,radar_score_avg,
                            radar_min,radar_score_min,
                            tab_space_q,
                            active_week, at_risk_week,
                            active_mouth, at_risk_mouth,
                            sql_time_num_rank,
                            risk_rule_rank,
                            user_health_ranking_avg,
                            user_health_ranking_min,
                            sqls)->str:

    """生成库html报告的离线压缩包"""

    file_name=cmdb['connect_name']+"_"+str(cmdb['cmdb_id'])+"_"+\
              arrow.now().strftime("%Y%m%d %H:%M:%S")+".tar.gz"
    path=settings.HEALTH_DIR+"/"+file_name

    v_page = print_html_script('O18 SQL审核管控平台')
    cmdb_print_html_body(v_page,cmdb)
    print_html_js(v_page)

    print_html_cmdb_basic_info(v_page,cmdb_q,tablespace_sum,cmdb_score)

    time_week = [arrow.get(x['date']).strftime('%Y-%m-%d') for x in active_week]
    active_week_data = [x['value'] for x in active_week]
    at_risk_week_data = [x['value'] for x in at_risk_week]
    time_mouth = [arrow.get(x['date']).strftime('%Y-%m-%d') for x in active_mouth]
    active_mouth_data = [x['value'] for x in active_mouth]
    at_risk_mouth_data = [x['value'] for x in at_risk_mouth]
    print_html_cmdb_sql_quality(v_page, time_week,
                                active_week_data,
                                at_risk_week_data,
                                time_mouth,
                                active_mouth_data,
                                at_risk_mouth_data)

    print_html_cmdb_radar(v_page,radar_avg,radar_score_avg,
                          radar_min,radar_score_min)

    # ts_usage_ratio = [str(round(x.to_dict()['usage_ratio'] * 100, 2)) + "%" for x in tab_space_q][:10]
    ts_usage_ratio = [x.to_dict()['usage_ratio'] for x in tab_space_q][:10]
    ts_name = [x.to_dict()['tablespace_name'] for x in tab_space_q][:10]
    sort_ts_usage_ration = sorted(ts_usage_ratio, reverse=False)
    print_html_cmdb_tab_space_use_ration(v_page, ts_name,ts_usage_ratio, sort_ts_usage_ration)

    x=[x['time']for x in sql_time_num_rank][:10]
    x.reverse()
    y=[y['sql_id']for y in sql_time_num_rank][:10]
    y.reverse()
    print_html_cmdb_sql_time_num(v_page,x,y)

    risk_name=[x['risk_name']for x in risk_rule_rank][:10]
    num=[y['num']for y in risk_rule_rank][:10]
    sort_num=sorted(num,reverse=False)
    print_html_cmdb_risk_rule_rank(v_page,risk_name,num,sort_num)


    x_avg=[x["schema"] for x in user_health_ranking_avg]
    y_avg=[round(y["num"]) for y in user_health_ranking_avg]
    x_min = [x["schema"] for x in user_health_ranking_min]
    y_min = [round(y["num"]) for y in user_health_ranking_min]
    print_html_cmdb_user_ranking(v_page,x_avg,y_avg,x_min,y_min)

    print_html_cmdb_sql_details(v_page,sqls)

    print_html_cmdb_js(v_page)

    v_page.printOut(f"html_report/sqlreviewcmdb.html")

    tar=tarfile.open(str(path),"w:gz")
    tar.add("html_report/css")
    tar.add("html_report/assets")
    tar.add("html_report/js")
    tar.add("html_report/sqlreviewcmdb.html")
    tar.close()

    path=os.path.join(settings.EXPORT_PREFIX_HEALTH, file_name)
    return path

