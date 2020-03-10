# -*- coding: utf-8 -*-
import json

import sqlparse

from past.utils.pyh import PyH, br
from utils import const
from utils.datetime_utils import *


def print_html_script(title):
    """
    加载离线页面的各种js和css脚本
    """

    # < linkhref = "http://fonts.googleapis.com/css?family=Open+Sans:300,400,600,700" rel = "stylesheet" >
    page = PyH(title)
    page << """
            <!-- ================== BEGIN BASE CSS STYLE ================== -->
            <style type="text/css">
			body, table, input, select, textarea
			{font:normal normal 8pt Verdana,Arial;text-decoration:none;color:#000000;}
			.s8 {font-size:8pt;color:#006699}
			.s9 {font-size:10pt;color:#006699}
			.s10 {font-size:14pt;color:#FC1212;}
			.s16 {border-width : 1px; border-color : #CCCC99;
					border-style: solid;color:#006699;font-size:8pt;}
			.s17 {border-width : 1px; border-color : #CCCC99;
				   border-style: solid;font-size:8pt;}
			.s27 {border-width : 1px; border-color : #CCCC99; border-style: solid;}
			.sqlDetailTitle {
				font-size:14px;
				border-top:0px
			}
			.sqlDetailContent {
				font-size:12px
			}
			.popover-content{
				width:1000px
			}
			</style>
            <link href="assets/plugins/jquery-ui/themes/base/minified/jquery-ui.min.css" rel="stylesheet" />
            <link href="assets/plugins/bootstrap/css/bootstrap.min.css" rel="stylesheet" />
            <link href="assets/plugins/font-awesome/css/font-awesome.min.css" rel="stylesheet" />
            <link href="assets/css/animate.min.css" rel="stylesheet" />
            <link href="assets/css/style.min.css" rel="stylesheet" />
            <link href="assets/css/style-responsive.min.css" rel="stylesheet" />
            <link href="assets/css/theme/default.css" rel="stylesheet" id="theme" />
            <!-- ================== END BASE CSS STYLE ================== -->

            <!-- ================== BEGIN PAGE LEVEL CSS STYLE ================== -->
            <link href="assets/plugins/jquery-jvectormap/jquery-jvectormap-1.2.2.css" rel="stylesheet" />

            <link href="assets/plugins/gritter/css/jquery.gritter.css" rel="stylesheet" />
            <link href="assets/plugins/morris/morris.css" rel="stylesheet" />
            <link href="css/lib/dataTables.bootstrap.css" rel="stylesheet">
            <link href="css/lib/dataTableStyle.css" rel="stylesheet">
            <link href="css/lib/jquery.treegrid.css" rel="stylcesheet">

            <link href="css/style.css" rel="stylesheet"/>
            <!-- ================== END PAGE LEVEL CSS STYLE ================== -->

            <!-- ================== BEGIN BASE JS ================== -->
            <script src="assets/plugins/pace/pace.min.js"></script>
            <!-- ================== END BASE JS ================== -->
            <!-- ================== BEGIN BASE JS ================== -->
            <script src="assets/plugins/jquery/jquery-1.9.1.min.js"></script>
            <script src="assets/plugins/jquery/jquery-migrate-1.1.0.min.js"></script>
            <script src="assets/plugins/jquery-ui/ui/minified/jquery-ui.min.js"></script>
            <script src="assets/plugins/bootstrap/js/bootstrap.min.js"></script>
            <!--[if lt IE 9]>
                <script src="assets/crossbrowserjs/html5shiv.js"></script>
                <script src="assets/crossbrowserjs/respond.min.js"></script>
                <script src="assets/crossbrowserjs/excanvas.min.js"></script>
            <![endif]-->
            <script src="assets/plugins/slimscroll/jquery.slimscroll.min.js"></script>
            <script src="assets/plugins/jquery-cookie/jquery.cookie.js"></script>
            <!-- ================== END BASE JS ================== -->

            <!-- ================== BEGIN PAGE LEVEL JS ================== -->
            <script src="assets/plugins/morris/raphael.min.js"></script>
            <script src="assets/plugins/morris/morris.js"></script>
            <script src="assets/plugins/jquery-jvectormap/jquery-jvectormap-1.2.2.min.js"></script>
            <script src="assets/plugins/jquery-jvectormap/jquery-jvectormap-world-merc-en.js"></script>

            <script src="assets/plugins/gritter/js/jquery.gritter.js"></script>
            <script src="js/lib/jquery.dataTables.js") }}"></script>
            <script src="js/lib/dataTables.bootstrap.js")}}"></script>
            <script src="js/lib/echarts.min.js"></script>
            <script src="js/lib/jquery.treegrid.js"></script>
            <script src="js/lib/jquery.treegrid.bootstrap3.js"></script>
            <script src="assets/js/apps.min.js"></script>
            <script src="js/task.js"></script>
            <!-- ================== END PAGE LEVEL JS ================== -->
            """
    return page
def cmdb_print_html_body(page, cmdb):
    page << """<body>
        <!-- begin #page-loader -->
        <div id="page-loader" class="fade in"><span class="spinner"></span></div>
        <!-- end #page-loader -->

        <!-- begin #page-container -->
        <div id="page-container" class="fade page-sidebar-fixed page-header-fixed">
            <!-- begin #header -->
            <div id="header" class="header navbar navbar-default navbar-fixed-top">
                <!-- begin container-fluid -->
                <div class="container-fluid">
                    <!-- begin mobile sidebar expand / collapse button -->
                    <div class="navbar-header">
                        <a href="#"><h3 class="top-title">
                        """+cmdb['connect_name']+"""
                        </h3></a>
                    </div>
                    <!-- end mobile sidebar expand / collapse button -->
                </div>
                <!-- end container-fluid -->
            </div>
            <!-- end #header -->

            <!-- begin #content -->
            <div id="content" class="content" style="background-color:white">
				<div class="row">
					<div class="span12" id="baseInfo"></div>
				</div>
				<div class="row">
					<div class="col-md-8">
						<div class="radio" style="float:right;font-size:16px">
						
						  <label class="radio-inline">
							<input type="radio" name="sqlQuality" id="zhou" value="zhou" checked>
							周
						  </label>
						  <label class="radio-inline">
							<input type="radio" name="sqlQuality" id="yue" value="yue">
							月
						  </label>
						</div>
						<br/><br/>
						<div id='sqlQualityZhou' class="col-md-12"></div>
						<div id='sqlQualityYue'  class="col-md-12"></div>
					</div>
					
					<div class="col-md-4">

						<div class="radio" style="float:right;font-size:16px">
						  <label class="radio-inline">
							<input type="radio" name="sqlHealth" id="avg" value="avg" checked>
							平均值
						  </label>
						  <label class="radio-inline">
							<input type="radio" name="sqlHealth" id="min" value="min">
							最小值
						  </label>
						</div>
						
						<div class="col-md-12" id="sqlHealthAvg">
						</div>
						
						<div class="col-md-12" id="sqlHealthMin">
						</div>
					</div>
				</div>
				
				<div class="row">
					<div class="col-md-4" id="tableUseSpace"></div>
					<div class="col-md-4" id="sqlBySum"></div>
					<div class="col-md-4" id="riskType"></div>
				</div>
				
				<div class="row">
						<div class="radio" style="float:right;font-size:16px">
						   <label class="radio-inline">
							<input type="radio" class="radio" name="userHealthy" id="UAvg" value="UAvg" checked>
							平均值
						  </label>
						  <label class="radio-inline">
							<input type="radio" class="radio" name="userHealthy" id="UMin" value="UMin">
							最小值
						  </label>
						</div>
						
						<br/><br/>
						<div class="col-md-12" id='userHealthyAvg'></div>
						<div class="col-md-12" id='userHealthyMin'></div>
				</div>
				
				<div class="row">
					<div class="col-md-12" id='sqlDetail'>
					<font class="s10">
						<br/>Report Details
					</font>
					<hr size="3" width="650" align="left"/><font style="width:300px;font-size:16px;">
					<!-- Modal -->
				
					<div class="modal fade" id="myModal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel">
					  <div class="modal-dialog" role="document">
						<div class="modal-content" style="width:800px">
						  <div class="modal-header">
							<button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
							<h4 class="modal-title" id="myModalLabel">执行计划</h4>
						  </div>
						  <div class="modal-body" >
							<pre  id="plana2"></pre>
						  </div>
						  <div class="modal-footer">
							<button type="button" class="btn btn-default" data-dismiss="modal">Close</button>
						  </div>
						</div>
					  </div>
					</div>
					</div>
				</div>
				
            <!-- begin scroll to top btn -->
<!--             <a href="javascript:;" class="btn btn-icon btn-circle btn-success btn-scroll-to-top fade" data-click="scroll-top"><i class="fa fa-angle-up"></i></a> -->
            <!-- end scroll to top btn -->
        </div>
        <!-- end page container -->
        </body>"""


def print_html_body(page, ipaddress, port, schema):
    """
    生成离线页面的body
    """
    page << """
        <body>
        <!-- begin #page-loader -->
        <div id="page-loader" class="fade in"><span class="spinner"></span></div>
        <!-- end #page-loader -->

        <!-- begin #page-container -->
        <div id="page-container" class="fade page-sidebar-fixed page-header-fixed">
            <!-- begin #header -->
            <div id="header" class="header navbar navbar-default navbar-fixed-top">
                <!-- begin container-fluid -->
                <div class="container-fluid">
                    <!-- begin mobile sidebar expand / collapse button -->
                    <div class="navbar-header">
                        <a href="#"><h3 class="top-title">
                        IPADDRESS:""" + ipaddress + """ PORT:""" + port + """ SCHEMA:""" + schema + """ sqlreview明细报告
                        </h3></a>
                    </div>
                    <!-- end mobile sidebar expand / collapse button -->
                </div>
                <!-- end container-fluid -->
            </div>
            <!-- end #header -->

            <!-- begin #content -->
            <div id="content" class="content">
            <div class="row">
            <!-- begin panel -->
            <div id="base">
            </div>
            <!-- end panel -->
            </div>
            <!-- end row -->
            </div>
            <!-- end #content -->


            <!-- begin scroll to top btn -->
            <a href="javascript:;" class="btn btn-icon btn-circle btn-success btn-scroll-to-top fade" data-click="scroll-top"><i class="fa fa-angle-up"></i></a>
            <!-- end scroll to top btn -->
        </div>
        <!-- end page container -->
        </body>
            """


def print_html_js(page):
    page << """
            <script>
                $(document).ready(function() {
                    App.init();
                });
            </script>
            """


def print_html_cmdb_js(page):

    page << """
            <script>
            $("#sqlHealthMin").hide();
			$("#sqlQualityYue").hide();
			$("#userHealthyMin").hide();
			
			$(".radio-inline").on("click",function(e){
				if(e.target.defaultValue == "yue"){
					$("#sqlQualityYue").show();
					$("#sqlQualityZhou").hide();

				 }
				 if(e.target.defaultValue == "zhou"){
					$("#sqlQualityZhou").show();
					$("#sqlQualityYue").hide();
				 }
				 if(e.target.defaultValue == "avg"){
					$("#sqlHealthAvg").show();
					$("#sqlHealthMin").hide();


				 }
				 if(e.target.defaultValue == "min"){
					$("#sqlHealthMin").show();
					$("#sqlHealthAvg").hide();

				 }if(e.target.defaultValue == "UAvg"){
					$("#userHealthyAvg").show();
					$("#userHealthyMin").hide();
				 }
				 if(e.target.defaultValue == "UMin"){
					$("#userHealthyMin").show();
					$("#userHealthyAvg").hide();

				 }
				 //要执行的代码
				 })</script>"""


def print_html_chart(total_score, page, rules):
    """
    生成页面中的饼图，依赖于百度的echarts
    """
    legend = ""
    pie_data = ""
    deduct_marks = 0
    for value in rules:
        legend += "'" + str(value[0]) + "',"
        pie_data += "{value:" + str(value[3]) + ", name: '" + value[0] + "'},"
        deduct_marks += float(value[3])
    score = (total_score - deduct_marks) / total_score * 100
    title = '规则总分: ' + str(round(score, 3))
    legend = "[" + legend + "]"
    pie_data = "[" + pie_data + "]"
    page << "<script>genCharts('" + title + "', '规则扣分详情'," + legend + ", " + pie_data + ")</script>"
    page << br()


def print_html_rule_table(page, ipaddress, port, schema, rules, score):
    """
    生成离线页面中的规则表格
    """
    title = ipaddress + " " + str(port) + " " + schema + "规则概览" + "(" + "规则总分" + str(round(score, 3)) + ")"
    data = ""
    columns = """[{ "title": "规则名称", "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {$(nTd).html("<a href='#" + oData[0] + "'>" + oData[0] + "</a>");}},{ "title": "规则描述"},{ "title": "违反次数"},{ "title": "扣分"}]"""
    for value in rules:
        data += "['" + value[0] + "','" + value[1] + "','" + str(value[2]) + "','" + str(value[3]) + "'],"
    page << "<script>genTable('#base', '" + title + "', 'rule_info_table', [" + data[
                                                                                :-1] + "], " + columns + ", 'rule_info', '1')</script>"
    page << br()


def print_html_rule_detail_table(page, result, rules, rule_type):
    """
    生成违反的规则的具体信息，由表格和文本构成
    """
    if rule_type.upper() == const.RULE_TYPE_SQLPLAN or \
            rule_type.upper() == const.RULE_TYPE_SQLSTAT:
        columns = """[
                        {
                            "title": "rulename",
                        },
                        {
                            "title": "sqlid",
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).html("<a href='#" + oData[0] + "-" + oData[1] + "-" +oData[3] + "'>" + oData[1] + "</a>");
                                }
                        },
                        {
                            "title": "sqltext"
                        },
                        {
                            "title": "plan_hashvalue"
                        },
                        {
                            "title": "pos"
                        },
                        {
                            "title": "object_name"
                        }
                    ]"""
    elif rule_type.upper() == const.RULE_TYPE_TEXT:
        columns = """[
                        {
                            "title": "rulename"
                        },
                        {
                            "title": "sqlid",
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).html("<a href='#" + oData[0] + "-" + oData[1] + "-1-v'>" + oData[1] + "</a>");
                                }
                        },
                        {
                            "title": "sqltext"
                        }
                    ]
            """
    else:
        assert 0

    # rule: [rule_name, rule_summary[rule_name][0], num, value.get("scores", 0), rule_summary[rule_name][2]]
    for rule in rules:
        rule_name, rule_summary, num, scores, solution = rule

        data = []
        for sql_dict in result[rule_name].get("sqls", []):
            sql_id = sql_dict["sql_id"]
            hash_value = sql_dict.get("plan_hash_value", "1")
            sql_text = sql_dict["sql_text"] or ""
            if rule_type.upper() == const.RULE_TYPE_SQLPLAN or \
                    rule_type.upper() == const.RULE_TYPE_SQLSTAT:
                sql_text = sql_text.replace("\n", " ").replace("'", "\"")
                obj_name = sql_dict["obj_name"] if sql_dict.get("obj_name", None) else "空"
                data.append(f"['{rule_name}', '{sql_id}', '{sql_text}', '{hash_value}', '', '{obj_name}']")

            elif rule_type.upper() == const.RULE_TYPE_TEXT:
                sql_text = sql_text[:40] if len(sql_text) > 40 else sql_text
                sql_text = sql_text.replace("\n", " ").replace("'", "\"")
                data.append(f"['{rule_name}', '{sql_id}', '{sql_text}']")

        if data:
            page << f"<script>genTable('#base', '{rule_name}', '{rule_name}_table', [{','.join(data)}], {columns},'{rule_name}', '3', \"{solution}\")</script>"
            page << br()
    page << br()


def print_html_rule_detail_info(page, result, rules):
    """
    sqlplan和sqlstat的具体信息
    rule_summary: [rule_summary, exclude_obj_type, solution]
    # rule: [rule_name, rule_summary[rule_name][0], num, value.get("scores", 0), rule_summary[rule_name][2]]
    """
    keys = set()
    for rule in rules:
        rule_name, rule_summary, num, scores, solution = rule
        for sql_dict in result[rule_name].get("sqls", []):

            # index_id = '-'.join(key.split("#")[:2])
            index_id = f"{sql_dict['sql_id']}-{sql_dict['plan_hash_value']}"
            div_id = rule_name + "-" + index_id

            if div_id in keys:
                continue

            keys.add(div_id)

            text_id = div_id + "-text"
            sql_fulltext = sqlparse.format(sql_dict["sql_text"] or "", reindent=True)
            sql_fulltext = json.dumps(sql_fulltext)

            obj_id = div_id + "-obj"
            obj_info = sql_dict["obj_info"]
            temp_obj_columns = []
            temp_obj_info = []
            if obj_info:
                for obj_key in obj_info.keys():
                    temp_obj_columns.append({"title": obj_key})
                    temp_obj_info.append(str(obj_info[obj_key]))
            temp_obj_info = json.dumps([temp_obj_info])
            temp_obj_columns = json.dumps(temp_obj_columns)

            stat_id = div_id + "-stat"
            stat_info = sql_dict["stat"]
            temp_stat_columns = []
            temp_stat_info = []
            if stat_info:
                for stat_key in stat_info.keys():
                    temp_stat_columns.append({"title": stat_key})
                    temp_stat_info.append(str(stat_info[stat_key]))
            temp_stat_info = json.dumps([temp_stat_info])
            temp_stat_columns = json.dumps(temp_stat_columns)

            plan_id = div_id + "-plan"
            plans = json.dumps(dt_to_str(sql_dict["plan"]))

            page << "<script>genMultiTable('#base', '" + div_id + "', '" + obj_id + \
            "', " + temp_obj_info + ", " + temp_obj_columns + ", '" + stat_id + "', " + \
            temp_stat_info + ", " + temp_stat_columns + ", '" + text_id + "', " + sql_fulltext + \
            ", '" + plan_id + "', " + plans + ", '" + div_id + "')</script>"
            page << br()


def print_html_rule_text_detail_info(page, results, rules):
    """
    文本类规则的具体信息
    """
    for rule in rules:
        for sql_dict in results[rule[0]].get("sqls", []):
            key = "#".join([sql_dict["sql_id"], "1", "v"])
            index_id = key.replace("#", "-")
            div_id = rule[0] + "-" + index_id
            text_id = div_id + "-text"
            sql_fulltext = sqlparse.format(sql_dict["sql_text"] or "",
                                           reindent=True)
            sql_fulltext = json.dumps(sql_fulltext)
            stat_info = sql_dict["stat"]
            temp_stat_columns = []
            temp_stat_info = []
            stat_id = div_id + "-stat"
            if stat_info:
                for stat_key in stat_info[0].keys():
                    temp_stat_columns.append({"title": stat_key})
                for stat in stat_info:
                    temp = []
                    for stat_key in stat.keys():
                        temp.append(str(stat[stat_key]))
                    temp_stat_info.append(temp)
            temp_stat_info = json.dumps(temp_stat_info)
            temp_stat_columns = json.dumps(temp_stat_columns)
            page << "<script>genMultiTextTable('#base', '" + div_id + "', '" + stat_id + \
            "', " + temp_stat_info + ", " + temp_stat_columns + ", '" + text_id + "', " + \
            sql_fulltext + ", '" + div_id + "')</script>"
            page << br()


def print_html_obj_detail_info(page, results, rules, rule_summary):
    """
    对象类规则的具体信息
    """
    for rule in rules:
        if results[rule[0]]["records"]:
            table_title = []
            records = []
            for data in rule_summary[rule[0]][2]:
                # table_title.append(data["parm_desc"])
                table_title.append({"title": data["parm_desc"]})
            for data in results[rule[0]]["records"]:
                records.append(data)
            table_title = json.dumps(table_title)
            records = json.dumps(records)
            page << "<script>genTable('#base', '" + rule[0] + "', '" + rule[
                0] + "_table', " + records + ", " + table_title + ",'" + rule[0] + \
            "', '3', '" + rule[4] + "')</script>"
            page << br()

    page << br()


def print_html_cmdb_basic_information(page, cmdb_q,tablespace_sum,cmdb_score):

    page << f"<script>genBaseInfo('#baseInfo','基本信息'," + str(cmdb_q) + ","+str(tablespace_sum)+","+str(cmdb_score)+")</script>"
    page << br()


def print_html_cmdb_sql_quality(page,time_week,active_week_data,
                                at_risk_week_data,time_mouth,
                                active_mouth_data,at_risk_mouth_data):

    legend=['活动sql', '风险sql']
    series_mouth=[
        {'name':'活动sql','type':'bar','stack':'数量',
      'label': {
          'show': 'true',
          'position': 'insideRight'
      },
      'data': active_mouth_data
      },
        {'name': '风险sql', 'type': 'bar', 'stack': '数量',
         'label': {
             'show': 'true',
             'position': 'insideRight'
         },
         'data': at_risk_mouth_data
         }
    ]
    series_week = [
        {'name': '活动sql', 'type': 'bar', 'stack': '数量',
         'label': {
             'show': 'true',
             'position': 'insideRight'
         },
         'data': active_week_data
         },
        {'name': '风险sql', 'type': 'bar', 'stack': '数量',
         'label': {
             'show': 'true',
             'position': 'insideRight'
         },
         'data': at_risk_week_data
         }
    ]
    page << '<script>genStackedHistogramChart("#sqlQualityYue","sql质量",'+str(legend)+','+str(time_mouth)+','+str(series_mouth)+')</script>'
    page << br()

    page << '<script>genStackedHistogramChart("#sqlQualityZhou","sql质量",'+str(legend)+','+str(time_week)+','+str(series_week)+')</script>'
    page << br()


def print_html_cmdb_radar(page,radar_avg,radar_score_avg,
                            radar_min,radar_score_min,):

    page << '<script>genRadarChart("#sqlHealthAvg","sqlHealthAvgDiv","SQL健康度雷达图",' + str(radar_avg) + ',' + str(radar_score_avg) + ')</script>'
    page << br()
    page << '<script>genRadarChart("#sqlHealthMin","sqlHealthMinDiv","SQL健康度雷达图",' + str(radar_min) + ',' + str(radar_score_min) + ')</script>'
    page << br()


def print_html_cmdb_sql_time_num(page, x, y):

    page << '<script>genSqlChart("#sqlBySum","sql语句总耗时",' + str(x) + ',' + str(y) + ')</script>'
    page << br()


def print_html_cmdb_tab_space_use_ration(page,ts_name,ts_usage_ratio,sort_ts_usage_ration):

    page << '<script>genBarChart("#tableUseSpace","tableUseSpaceDiv","表空间使用率",' + str(ts_name) + ',' + str(ts_usage_ratio) + ','+str(sort_ts_usage_ration)+')</script>'
    page << br()


def print_html_cmdb_risk_rule_rank(page, risk_name, num,sort_num):

    page << '<script>genBarChart("#riskType","riskTypeDiv","风险类型",'+ str(risk_name) + ',' + str(num) + ','+str(sort_num)+')</script>'
    page << br()


def print_html_cmdb_user_ranking(page, x_avg,y_avg,x_min,y_min):

    page << "<script>genUserHealthyChart('#userHealthyMin','userHealthyMinDiv','用户健康度排名',''," + str(x_min) + "," + str(y_min) + ")</script>"
    page << br()

    page << "<script>genUserHealthyChart('#userHealthyAvg','userHealthyAvgDiv','用户健康度排名',''," + str(x_avg) + "," + str(y_avg) + ")</script>"
    page << br()

def print_html_cmdb_sql_details(page,sqls):

    for sql in sqls:
        page << "<script>genSqlDetail('#sqlDetail','sql详情',"+str(sql)+")</script>"
        page << br()


