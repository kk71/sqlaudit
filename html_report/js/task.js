function bytesToSize(bytes) {
  if (bytes === 0) return '0 B';
  var k = 1000, // or 1024
      sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
      i = Math.floor(Math.log(bytes) / Math.log(k));

 return (bytes / Math.pow(k, i)).toPrecision(3) + ' ' + sizes[i];
// console.log( (bytes / Math.pow(k, i)).toPrecision(3) + ' ' + sizes[i])

}
function genTable(domid, title, table_id, data, columns, div_id, flag){
    var desc = arguments[7] ? arguments[7] : "";
    var order = 0
    if (flag === "1" || flag === "2"){
      if (flag === "1") {
        order = 3;
      }
      $(domid).append('<div id=\"' + div_id + '\">\
                    <div class=\"panel panel-inverse\">\
                      <div class=\"panel-heading\">\
                          <h4 class=\"panel-title\">' + title + '</h4>\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <table class=\"table table-striped table-bordered table_rule\" id=\"' + table_id +'\"></table>\
                        </div>\
                      </div>\
              </div>')
    }
    else if(flag === "3"){
      $(domid).append('<div id=\"' + div_id + '\">\
                    <div class=\"panel panel-inverse\">\
                      <div class=\"panel-heading\">\
                          <h4 class=\"panel-title\">' + title + '</h4>\
                      </div>\
                      <div class=\"panel-body\">\
                      <button class=\"btn btn-primary m-r-5 m-b-5 accordion-toggle accordion-toggle-styled collapsed\" data-toggle=\"collapse\" href=\"#solution_' + title + '\">解决方案</button>\
                        <div id=\"solution_' + title + '\" class=\"panel panel-body panel-collapse collapse\">' + desc + '</div>\
                          <div class=\"table-responsive\">\
                          <table class=\"table table-striped table-bordered table_rule\" id=\"' + table_id +'\"></table>\
                        </div>\
                      </div>\
              </div>')
    }
    var table = $("#" + table_id).dataTable({
        "data": data,
        "columns": columns,
        "order": [[ order, "desc" ]],
        "language": {
           "sProcessing": "处理中...",
           "sLengthMenu": "显示 _MENU_ 项结果",
           "sZeroRecords": "没有匹配结果",
           "sInfo": "显示第 _START_ 至 _END_ 项结果，共 _TOTAL_ 项",
           "sInfoEmpty": "显示第 0 至 0 项结果，共 0 项",
           "sInfoFiltered": "(由 _MAX_ 项结果过滤)",
           "sInfoPostFix": "",
           "sSearch": "搜索:",
           "sUrl": "",
           "sEmptyTable": "表中数据为空",
           "sLoadingRecords": "载入中...",
           "sInfoThousands": ",",
           "oPaginate": {
               "sFirst": "首页",
               "sPrevious": "上页",
               "sNext": "下页",
               "sLast": "末页"
           }
        },
    });
    return table;
}

function genCharts(domid,title, subtext, legend, rule_mark){
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
    $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"rule_mark_pie\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')

    function gen() {
            option = {
                    title : {
                        text: title,
                        subtext: subtext,
                        x: "center"
                    },
                    tooltip : {
                        trigger: 'items',
                        formatter: "{a} <br/>{b} : {c} ({d}分)"
                    },
                    legend: {
                        data: legend,
                        orient : 'vertical',
                        x : 'left',
                    },
                    toolbox: {
                        show : true,
                        feature : {
                            // mark : {show: true},
                            // dataView : {show: true, readOnly: false},
                            restore : {show: true},
                            saveAsImage : {show: true},
                            magicType : {
                            show: true,
                            type: ['pie', 'funnel'],
                            option: {
                                funnel: {
                                    x: '50%',
                                    width: '50%',
                                    funnelAlign: 'left',
                                    max: 1548
                                }
                            }
                        },
                      }
                    },
                    calculable : true,
                    series : [
                        {
                            name: "规则扣分图",
                            type:'pie',
                            radius : '55%',
                            center: ['60%', '60%'],
                            data: rule_mark,
                        }
                    ]
                };
        var myChart = echarts.init(document.getElementById("rule_mark_pie"));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}
//环形图-圆环图类似规则扣分图
function genRingChart(domid,title, subtext, legend, rule_mark){
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
   $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"rule_mark_pie\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')

    function gen() {
            option = {
                    title : {
                        text: title,
                        subtext: subtext,
                        x: "center"
                    },
                    tooltip : {
                        trigger: 'items',
                        formatter: "{a} <br/>{b} : {c} ({d}分)"
                    },
                    legend: {
                        data: legend,
                        orient : 'vertical',
                        x : 'left',
                    },
                    toolbox: {
                        show : true,
                        feature : {
                            // mark : {show: true},
                            // dataView : {show: true, readOnly: false},
                            restore : {show: true},
                            saveAsImage : {show: true},
                            magicType : {
                            show: true,
                            type: ['pie', 'funnel'],
                            option: {
                                funnel: {
                                    x: '50%',
                                    width: '50%',
                                    funnelAlign: 'left',
                                    max: 1548
                                }
                            }
                        },
                      }
                    },
                    calculable : true,
                    series : [
                        {
                            name: "规则扣分图",
                            type:'pie',
                            radius : ['50%', '70%'],
						    itemStyle : {
								normal : {
									label : {
										show : false
									},
									labelLine : {
										show : false
									}
								},
								emphasis : {
									label : {
										show : true,
										position : 'center',
										textStyle : {
											fontSize : '30',
											fontWeight : 'bold'
										}
									}
								}
							},
                            data: rule_mark,
                        }
                    ]
                };
        var myChart = echarts.init(document.getElementById("rule_mark_pie"));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}
//折线面积图
function genAreaChart(domid,title, subtext, xData,yData){
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
    $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"dbTime_mark_pie\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')

    function gen() {
            option = {
                    title : {
                        text: title,
                        subtext: subtext,
                        x: "center"
                    },
                    tooltip : {
                        trigger: 'axis'
                    },
                    toolbox: {
						show : true,
						feature : {
							mark : {show: true},
							dataView : {show: true, readOnly: false},
							magicType : {show: true, type: ['line', 'bar', 'stack', 'tiled']},
							restore : {show: true},
							saveAsImage : {show: true}
						}
					},
                    calculable : true,
                    xAxis : [
						{
							type : 'category',
							boundaryGap : false,
							data : xData
						}
					],
					yAxis : [
						{
							type : 'value'
						}
					],
					series : [
						{
							name:'压力值',
							type:'line',
							smooth:true,
							itemStyle: {normal: {areaStyle: {type: 'default'}}},
							data:yData
						},
						
					]
                };
        var myChart = echarts.init(document.getElementById("dbTime_mark_pie"));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}
//竖柱状图 用户健康排名图表可用  title 图表名称 例如此图表示用户排名  xData 表示x轴坐标 数组格式 yData 表示y轴坐标 数组格式 subtext  可以为空
function genUserHealthyChart(domid,divid,title, subtext, xData,yData){
	
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
    $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"'+divid+'\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')

    function gen() {
            option = {
                    title : {
                        text: title,
                        subtext: subtext,
                        x: "center"
                    },
                    tooltip : {
                        trigger: 'axis'
                    },
                     toolbox: {
						show : true,
						feature : {
							mark : {show: true},
							dataView : {show: true, readOnly: false},
							magicType : {show: true, type: ['line', 'bar']},
							restore : {show: true},
							saveAsImage : {show: true}
						}
					},
                    calculable : true,
                    xAxis : [
						{
							type : 'category',
							data : xData
						}
					],
					yAxis : [
						{
							type : 'value'
						}
					],
					    series : [
							{
							   name:'分数',
								type:'bar',
								data:yData,
								markPoint : {
									data : [
										{type : 'max', name: '最大值'},
										{type : 'min', name: '最小值'}
									]
								},
								markLine : {
									data : [
										{type : 'average', name: '平均值'}
									]
								}
							},
						   
						]
                };
        var myChart = echarts.init(document.getElementById(divid));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}
function genMultiTable(domid, title, obj_id, obj_data, obj_columns, stat_id, stat_data, stat_columns, text_id, text, plan_id, plan, div_id){
    var sql_detail = '<div id=\"' + div_id + '\">\
                        <div class=\"panel\">\
                          <div class=\"panel-heading\">\
                          </div>\
                          <div class=\"panel-body\">\
                              <div class=\"table-responsive\">\
                                <textarea id=\"' + text_id + '\" rows=\"8\" class=\"form-control\"></textarea>\
                              </div>\
                      '
    var planhtml = '<div class=\"table-responsive\">\
                            <table class=\"table table-striped table-bordered table_rule\" id=\"' + plan_id + '\"><thead><tr><th>OPERATION</th><th>OPTIONS</th><th>OBJECT_OWNER</th><th>OBJECT_NAME</th><th>COST</th></tr></thread></table>\
                          </div>\
                          '
    if (obj_columns.length){
      obj_table = '<div class=\"table-responsive\"><table class=\"table table-striped table-bordered table_rule\" id=\"' + obj_id +'\"></table></div>'
      sql_detail += obj_table
    }
    if(stat_columns.length){
      stat_table = '<div class=\"table-responsive\">\
                              <table class=\"table table-striped table-bordered table_rule\" id=\"' + stat_id +'\"></table>\
                              </div>'
      sql_detail += stat_table
    }
    sql_detail += "</div></div>"
    $(domid).append(sql_detail)
    var language = {
           "sProcessing": "处理中...",
           "sLengthMenu": "显示 _MENU_ 项结果",
           "sZeroRecords": "没有匹配结果",
           "sInfo": "显示第 _START_ 至 _END_ 项结果，共 _TOTAL_ 项",
           "sInfoEmpty": "显示第 0 至 0 项结果，共 0 项",
           "sInfoFiltered": "(由 _MAX_ 项结果过滤)",
           "sInfoPostFix": "",
           "sSearch": "搜索:",
           "sUrl": "",
           "sEmptyTable": "表中数据为空",
           "sLoadingRecords": "载入中...",
           "sInfoThousands": ",",
           "oPaginate": {
               "sFirst": "首页",
               "sPrevious": "上页",
               "sNext": "下页",
               "sLast": "末页"
           }
        }
    if (obj_columns.length){
        $("#" + obj_id).dataTable({
          "data": obj_data,
          "columns": obj_columns,
          "paging":   false,
          "searching": false,
          "language": language
      });
    }
    if(stat_columns.length){
        $("#" + stat_id).dataTable({
          "data": stat_data,
          "columns": stat_columns,
          "paging":   false,
          "searching": false,
          "language": language
      });
    }
    $("#" + text_id).val(text);

    if(plan != null && plan != undefined && plan != "" && plan != "[]"){
        $('#' + div_id + ' .panel-body').append(planhtml)
        $.each(plan, function(key, val){
              var td = "<td>" + val["OPERATION_DISPLAY"] + "</td><td>" + val["OPTIONS"] + "</td><td>" + val["OBJECT_OWNER"] + "</td><td>" + val["OBJECT_NAME"] + "</td><td>" + val["COST"] + "</td>";
              if (val["PARENT_ID"] === null){
                var tr=$("<tr></tr>").addClass("treegrid-" + (parseInt(val["ID"]) + 1)).appendTo($('#' + plan_id)).html(td);
              }
              else{
                var tr=$("<tr></tr>").addClass("treegrid-" + (parseInt(val["ID"]) + 1)).addClass("treegrid-parent-" + (parseInt(val["PARENT_ID"]) + 1)).appendTo($('#' + plan_id)).html(td);
              }
            });
        $("#" + plan_id).treegrid({
                expanderExpandedClass: 'glyphicon glyphicon-minus',
                expanderCollapsedClass: 'glyphicon glyphicon-plus'
        });
    }
}

function genMultiTextTable(domid, title, stat_id, stat_data, stat_columns, text_id, text, div_id){
    var sql_detail = '<div id=\"' + div_id + '\">\
                        <div class=\"panel\">\
                          <div class=\"panel-heading\">\
                          </div>\
                          <div class=\"panel-body\">\
                              <div class=\"table-responsive\">\
                                <textarea id=\"' + text_id + '\" rows=\"8\" class=\"form-control\"></textarea>\
                              </div>\
                      '
    if(stat_columns.length){
      stat_table = '<div class=\"table-responsive\">\
                              <table class=\"table table-striped table-bordered table_rule\" id=\"' + stat_id +'\"></table>\
                              </div>\
                              '
      sql_detail += stat_table
    }
    sql_detail += "</div></div>"
    $(domid).append(sql_detail)
    var language = {
           "sProcessing": "处理中...",
           "sLengthMenu": "显示 _MENU_ 项结果",
           "sZeroRecords": "没有匹配结果",
           "sInfo": "显示第 _START_ 至 _END_ 项结果，共 _TOTAL_ 项",
           "sInfoEmpty": "显示第 0 至 0 项结果，共 0 项",
           "sInfoFiltered": "(由 _MAX_ 项结果过滤)",
           "sInfoPostFix": "",
           "sSearch": "搜索:",
           "sUrl": "",
           "sEmptyTable": "表中数据为空",
           "sLoadingRecords": "载入中...",
           "sInfoThousands": ",",
           "oPaginate": {
               "sFirst": "首页",
               "sPrevious": "上页",
               "sNext": "下页",
               "sLast": "末页"
           }
        }
    if(stat_columns.length){
        $("#" + stat_id).dataTable({
          "data": stat_data,
          "columns": stat_columns,
          "paging":   false,
          "searching": false,
          "language": language
      });
    }
    $("#" + text_id).val(text);
}
//横柱状图（对应sql语句总耗时）
//title 对应图表名称 例如此图表示sql 语句总耗时，xData x坐标 数组格式，yData y坐标 数组格式
//title:'sql 总耗时',xData:[120, 200, 150, 80, 70, 110, 130],yData:['0jhqydmsxjdgx', '4tm4zpn0khcqf', '20suta9bjmp0r', '670z7a9f6b8n5', 'ampq3jthv1jkk', '4uyf3f511jn91', 'ajvqx9uthq8mq'])
function genSqlChart(domid,title, xData,yData){
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
    $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"sql_time_by_sum\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')

    function gen() {
           option = {
			title: {
				text: title,
			},
			tooltip: {
				trigger: 'axis',
				axisPointer: {
					type: 'shadow'
				}
			},
			grid: {
				left: '3%',
				right: '4%',
				bottom: '3%',
				containLabel: true
			},
			xAxis: {
				type: 'value',
				boundaryGap: [0, 0.01]
			},
			yAxis: {
				type: 'category',
				data: yData
			},
			series: [
				{
					name: '总耗时',
					type: 'bar',
					data: xData,
                    label: {
					show: true,
					position: 'inside',
					formatter:function (val) {
						return val.value+'s'


                  }
				},
				}
			]
		};


        var myChart = echarts.init(document.getElementById("sql_time_by_sum"));
        myChart.setOption(option);
        myChart.on('click', function (params) {
			window.location.href = '#'+params.name
		});
        // return myChart
        // });
        return myChart;
    }
    return gen();
}

//饼图（对应表空间使用率）
//title 对应图表名称 例如此图表示sql 语句总耗shi data  为数据格式 
//data: [{value: 335, name: '直接访问'},{value: 310, name: '邮件营销'},{value: 234, name: '联盟广告'},{value: 135, name: '视频广告'},{value: 1548, name: '搜索引擎'}],
function genPieChart(domid,title, data){
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
    $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"pieChart\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')

    function gen() {
           option = {
			title: {
				text: title,
				left: 'center'
			},
			tooltip: {
				trigger: 'item',
				formatter: '{a} <br/>{b} : {c} ({d}%)'
			},
			
			series: [
				{
					name: '姓名',
					type: 'pie',
					radius: '55%',
					center: ['40%', '50%'],
					data: data,
					emphasis: {
						itemStyle: {
							shadowBlur: 10,
							shadowOffsetX: 0,
							shadowColor: 'rgba(0, 0, 0, 0.5)'
						}
					}
				}
			]
		};


        var myChart = echarts.init(document.getElementById("pieChart"));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}
//雷达图 indicator 格式 [{ name: '对象', max: 100},{ name: '文本', max: 100},{ name: '统计', max: 100},{ name: '执行计划', max: 100}] max 根据实际要求设置大小设置最大值
//data [10, 20, 50, 30] 依次对应上述排序数值
function genRadarChart(domid,divid,title, indicator,data){
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
   
	   $(domid).append('<div class=\"panel\">\
			  <div class=\"panel-heading\">\
			  </div>\
			  <div class=\"panel-body\">\
				  <div class=\"table-responsive\">\
				  <div  id=\"'+divid+'\" style=\"height:400px\"></div>\
				</div>\
			  </div>\
	  </div>')

    function gen() {
           option = {
			title: {
				text: title
			},
			tooltip: {},
			radar: {
				// shape: 'circle',
				name: {
					textStyle: {
						color: '#fff',
						backgroundColor: '#999',
						borderRadius: 3,
						padding: [3, 5]
					}
				},
				indicator: indicator
			},
			series: [{
				name: '',
				type: 'radar',
				// areaStyle: {normal: {}},
				data: [
					{
						value: data
					}
				]
			}]
		};


        var myChart = echarts.init(document.getElementById(divid));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}

// 进度条图形（用于风险类型图表） 参数说明 title：名称 表示此图名称   list: ['记录长度定义过长', '不包含时间戳字段的表', '组合索引数量过多', '没有主键的表', '序列CACHESIZE过小', '索引的聚簇因子', '包含有大字段类型的表', '索引数量过多'],list 
//data :[63, 63, 63, 60, 50, 40,10,2]  依次对应list dataY是data的逆序
// 按顺序排列从大到小
//list 字段对应 risk_rule_rank 数组里面的 risk_name    data 对应 risk_rule_rank 数组里面的 risk_name num
//https://gallery.echartsjs.com/editor.html?c=xgqdKhn5c
function genBarChart(domid,divid,title, list,data,dataY){
    var myChart;
	var bs;
	if(title=='表空间使用率'){
		bs=1;
	}
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
    $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"'+divid+'\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')
	var charts = { // 按顺序排列从大到小
		cityList:list,
		cityData:data,
		cityDataY:dataY
		}
	var top10CityList = charts.cityList
	var top10CityData = charts.cityData
	var top10CityDataY = charts.cityDataY
	var color = ['rgba(213,58,58']

	let lineY = []
	for (var i = 0; i < charts.cityList.length; i++) {
	  var x = i
	  if (x > color.length - 1) {
		x = color.length - 1
	  }
	  var data = {
		name: charts.cityList[i],
		color: color[0] + ')',
		value: top10CityData[i],
		itemStyle: {
		  normal: {
			show: true,
			color: new echarts.graphic.LinearGradient(0, 0, 1, 0, [{
			  offset: 0,
			  color: color[0] + ', 0.3)'
			}, {
			  offset: 1,
			  color: color[0] + ', 1)'
			}], false),
			barBorderRadius: 10
		  },
		  emphasis: {
			shadowBlur: 10,
			shadowColor: 'rgba(0, 0, 0, 0.1)'
		  }
		}
	  }
	  lineY.push(data)
	}
	console.log(lineY)

    function gen() {
           option= {
			  title: {
				show: true,
				text:title,
			  },
			  tooltip: {
				trigger: 'item'
			  },
			  grid: {
				borderWidth: 0,
				top: '10%',
				left: '5%',
				right: '15%',
				bottom: '3%'
			  },
			  color: color,
			  yAxis: [{
                type: 'category',
                inverse: true,
                axisTick: {
                  show: false
                },
                axisLine: {
                  show: false
                },
                axisLabel: {
                  show: false,
                  inside: false
                },
                data: top10CityList
              }, {
                type: 'category',
                axisLine: {
                  show: false
                },
                axisTick: {
                  show: false
                },
                axisLabel: {
                  show: true,
                  inside: false,
                  textStyle: {
                    color: '#000',
                    fontSize: '14',
                    fontFamily: 'PingFangSC-Regular'
                  },
                  formatter: function (val) {
					  if(bs==1){
						return (val*100).toFixed(2)+'%'
					  }else{
						  return `${val}`
					  }
                    
                  }
                },
                splitArea: {
                  show: false
                },
                splitLine: {
                  show: false
                },
                data: top10CityDataY
              }],
			  xAxis: {
				type: 'value',
				axisTick: {
				  show: false
				},
				axisLine: {
				  show: false
				},
				splitLine: {
				  show: false
				},
				axisLabel: {
				  show: false
				}
			  },
			  series: [{
				name: '',
				type: 'bar',
				zlevel: 2,
				barWidth: '10px',
				data: lineY,
				animationDuration: 1500,
				label: {
				  normal: {
					color: '#262626',
					show: true,
					position: [0, '-24px'],
					textStyle: {
					  fontSize: 16
					},
					formatter: function (a, b) {
					  return a.name
					}
				  }
				}
			  }],
			  animationEasing: 'cubicOut'
			};


        var myChart = echarts.init(document.getElementById(divid));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}

//基本信息 domid 写死为#base  title 名称  databaseInfo
function genBaseInfo(domid,title,databaseInfo,tablespace_sum){
 $(domid).append('<div class=\"container-fluid \" style="font-size:16px; background-color:white">\
                      <div class=\"row\"><div class=\"col-xs-6 col-sm-12\"><strong>' + title + '</strong></div></div>'+
       '<div class=\"row\" style=\"margin-top: 8px;\">' +
       '<div class=\"col-xs-6 col-sm-3\">'+'数据库名称：' +databaseInfo.connect_name+
       '</div><div class=\"col-xs-6 col-sm-3\" >' + '数据库类型：Oracle数据库' +
       '</div><div class=\"col-xs-6 col-sm-3\">' + '业务模型：' +databaseInfo.db_model+
	   '</div><div class=\"col-xs-6 col-sm-3\">'+ '容量使用率(已使用/剩余)：' + bytesToSize(tablespace_sum['used'])+'/' +bytesToSize(tablespace_sum['free'])+
       '</div></div><div class=\"row\" style=\"margin-top: 8px;\">' +
       '<div class=\"col-xs-6 col-sm-3\">'+'评分日期：' +databaseInfo.score_time+
       '</div><div class=\"col-xs-6 col-sm-3\">' + '综合评分数：' +databaseInfo.score+
       '</div><div class=\"col-xs-6 col-sm-3\">' + 'SQL评分数：' +databaseInfo.sql_score+
       '</div><div class=\"col-xs-6 col-sm-3\">'+ '对象评分数：' +databaseInfo.obj_score+
       '<br/><br/></div></div></div>'
                      )
}
//堆叠柱状图（对应sql质量）
//title 对应图表名称 例如此图表示sql 语句总耗时，xData x坐标 数组格式，yData y坐标 数组格式
//title:'sql 总耗时',yData:[120, 200, 150, 80, 70, 110, 130],xData:['周一', '周二', '周三', '周四', '周五', '周六', '周日']
//legend:['直接访问', '邮件营销', '联盟广告', '视频广告', '搜索引擎']
//series:[
        /* {
            name: '直接访问',
            type: 'bar',
            stack: '总量',
            label: {
                show: true,
                position: 'insideRight'
            },
            data: [320, 302, 301, 334, 390, 330, 320]
			},
			{
				name: '邮件营销',
				type: 'bar',
				stack: '数量',
				label: {
					show: true,
					position: 'insideRight'
				},
				data: [120, 132, 101, 134, 90, 230, 210]
			}
			
		] */
function genStackedHistogramChart(domid,title,legend, xData,series){
    var myChart;
    // require(['echarts', 'echarts/chart/pie', 'echarts/chart/funnel', 'echarts/chart/line'],
    $(domid).append('<div class=\"panel\">\
                      <div class=\"panel-heading\">\
                      </div>\
                      <div class=\"panel-body\">\
                          <div class=\"table-responsive\">\
                          <div  id=\"stackedHistogramChar\" style=\"height:400px\"></div>\
                        </div>\
                      </div>\
              </div>')

    function gen() {
           option = {
			   title: {
				text: title,
				left: 'left'
			},
    tooltip: {
        trigger: 'axis',
        axisPointer: {            // 坐标轴指示器，坐标轴触发有效
            type: 'shadow'        // 默认为直线，可选为：'line' | 'shadow'
        }
    },
    legend: {
        data: legend
    },
    grid: {
        left: '3%',
        right: '4%',
        bottom: '3%',
        containLabel: true
    },
    xAxis: {
        type: 'category',
		data: xData

    },
    yAxis: {
        type: 'value',
    },
    series:series
	};


        var myChart = echarts.init(document.getElementById("stackedHistogramChar"));
        myChart.setOption(option);
        // return myChart
        // });
        return myChart;
    }
    return gen();
}

//基本信息 domid 写死为#base  title 名称  databaseInfo 数据库对象信息
function genSqlDetail(domid,title,databaseInfo){
$(domid).append();
 $(domid).append('<font style="width:300px;font-size:16px;"><a name="'+databaseInfo.sql_id+'"/><br/> <b> SQL Details: </b></font>\
				<table class=\"table\" cellspacing="0" style="width:400px;">'+
				'<tr style="textAlign:left"><td colspan=\"2\"><font style="font-size:14px;"><br/> <b> 基本信息&nbsp: </b></font></td></tr>\
				<tr><td style="border-top:0px" class=\"sqlDetailTitle\">sql_id</td><td style="border-top:0px" class=\"sqlDetailContent\">:&nbsp &nbsp'+databaseInfo.sql_id+'</td></tr>\
				<tr><td style="border-top:0px" class=\"sqlDetailTitle\">执行用户:</td><td style="border-top:0px" class=\"sqlDetailContent\">:&nbsp &nbsp'+databaseInfo.schema_name+'</td></tr>'+
				'<tr><td style="border-top:0px" class=\"sqlDetailTitle\">首次出现时间</td>\
				<td style="border-top:0px" class=\"sqlDetailContent\">:&nbsp &nbsp'+databaseInfo.first_appearance+'</td></tr>\
				<tr><td style="border-top:0px" class=\"sqlDetailTitle\">最后活动时间:</td>\
				<td style="border-top:0px" class=\"sqlDetailContent\">:&nbsp &nbsp'+databaseInfo.last_appearance+'</td></tr>\
				<tr><td style="border-top:0px" class=\"sqlDetailTitle\">执行次数:</td>\
				<td style="border-top:0px" class=\"sqlDetailContent\">:&nbsp &nbsp'+databaseInfo.sql_stat.executions_total.toFixed(0)+'</td></tr>\
				<tr><td style="border-top:0px" class=\"sqlDetailTitle\">执行时间:</td>\
				<td style="border-top:0px" class=\"sqlDetailContent\">:&nbsp &nbsp'+databaseInfo.sql_stat.elapsed_time_delta.toFixed(2)+'</td></tr>\
				<tr><td style="border-top:0px" class=\"sqlDetailTitle\">sql_text</td>\
				<td style="border-top:0px" class=\"sqlDetailContent\">:&nbsp &nbsp'+databaseInfo.longer_sql_text_prettified+'</td></tr></table>\
				<table class=\"table\" cellspacing="0" style="width:1200px;">'+
				'<tr style="textAlign:left"><td colspan=\"5\"><font style="font-size:14px;"><br/> <b> 风险点&nbsp: </b></font></td></tr>\
				<tr><td class=\"sqlDetailTitle\">风险</td><td class=\"sqlDetailTitle\">影响程度</td><td class=\"sqlDetailTitle\">风险说明</td>\
				<td class=\"sqlDetailTitle\">查看建议</td></tr>'+genSqlRiskPoint(databaseInfo.risk_rules)+
				'</table>\
				<table class=\"table\" cellspacing="0" style="width:1200px;">'+
				'<tr style="textAlign:left"><td colspan=\"5\"><font style="font-size:14px;"><br/> <b> 执行计划&nbsp: </b></font></td></tr>\
				<tr><td class=\"sqlDetailTitle\">Hash值</td><td class=\"sqlDetailTitle\">cpu时间(s)</td><td class=\"sqlDetailTitle\">平均cpu时间(s)</td>\
				<td class=\"sqlDetailTitle\">执行耗时</td><td class=\"sqlDetailTitle\">平均执行耗时</td><td class=\"sqlDetailTitle\">操作</td></tr>'+genSqlPlan(databaseInfo.plans)+
				'</table>'

                      )
}
//拼接行
function genSqlRiskPoint(riskPoint){
	var sb="";
	for(var j = 0,len = riskPoint.length; j < len; j++){
		sb=sb+'<tr><td>'+riskPoint[j].rule_name+'</td><td>'+riskPoint[j].level+'</td><td>'+riskPoint[j].rule_desc+'</td><td>'+riskPoint[j].rule_solution+'</td></tr>';
	}
	return sb;
}
//拼接行
function genSqlPlan(sqlPlan){
	var sb="";
	for(var j = 0,len = sqlPlan.length; j < len; j++){
		sb=sb+
		'<tr><td>'+sqlPlan[j].plan_hash_value+
		'</td><td>'+sqlPlan[j].cpu_time_total+
		'</td><td>'+sqlPlan[j].cpu_time_delta+
		'</td><td>'+sqlPlan[j].elapsed_time_total+
		'</td><td>'+sqlPlan[j].elapsed_time_delta+
		'</td><td><a id="a-plan'+j+'" data-toggle="modal" value="'+sqlPlan[j].sql_plan+'" onclick=\"roleupdate('+j+')\"  data-target=\"#myModal\">查看执行计划</a></td></tr>';
	}
	return sb;
}
function roleupdate(j){
	var str = $("#a-plan"+j).attr('value');
	console.info('str:'+str);

	//$("#plana2").innerHTML = str;

	document.getElementById("plana2").innerHTML=str;


}


