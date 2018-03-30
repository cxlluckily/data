var timer=null;//定时器
//window.clearInterval(timer); //清除定时
$(function() {
	//地铁路网图展示设备状态
	new DeviceStatusMap();
   
	Sub.subscribe({
	   topic : "monitoring-init-time",
	   pattern : false,
	   handle : function(data) {
		   $("#record_realtime").html(data.lastTime);
	   }
   });

   // 故障实时记录
	Sub.subscribe({
	   topic : "device_failure_4401",
	   pattern : false,
	   handle : function(data) {
		   var data_html = "";
		   for (i in data) {
			   var status_value = data[i].status_value;
			   var status_name = '';
			   var cls = '';
			   if(status_value == '1'){
				   status_name = '停止运营';
				   cls = 'gz_stop';
			   }
			   if(status_value == '2'){
				   status_name = '维修模式';
				   cls = 'gz_xiu';
			   }
			   if(status_value == '3'){
				   status_name = '故障模式';
				   cls = 'gz_gz';
			   }
			   if(status_value == '4'){
				   status_name = '通讯中断';
				   cls = 'gz_off';
			   }
			   if(status_value == '9'){
				   status_name = '离线状态';
				   cls = '';
			   }
			   data_html += "<tr><td><b class='" + cls + "'>" + status_name
					   + "</b></td><td>" + data[i].device_id
					   + "</td><td>" + data[i].station_name
					   + "</td></tr>";
		   }
		   $("table.clf_table2").html(data_html);

	   }
   });
	//故障排行
	failureRank();
   
   $("#recordMax").click(function() {
		//打开对话框
		openDialog(".recordDialog", function(dialog) {
			
			//初始化时间控件
			$(dialog).find(".calendartxt").datepicker({
				changeMonth : true,
				changeYear : true,
				dateFormat: "yy-mm-dd"
			});
			
			$(dialog).find("#startBtn").click(function() {
				$(dialog).find(".calendartxt:eq(0)").datepicker('show');
			});
			
			$(dialog).find("#endBtn").click(function() {
				$(dialog).find(".calendartxt:eq(1)").datepicker('show');
			});
						
			//加载站点名
			var availableStation = new Array();
			for(var i in stationMap){
				availableStation[i] = stationMap[i].name;
			}
			$(dialog).find( "#stationName" ).autocomplete({
				source :  availableStation,
				minLength : 0
			});
			
/*			//使用表中数据加载站名和设备编号（卡）
			var availableStation = new Set();
			var availableDevice = new Set();
			$("table.clf_table2 tr").each(function() {
				availableDevice.add($(this).find("td:eq(1)").text());
				availableStation.add($(this).find("td:eq(2)").text()); 
			});
			console.info(availableDevice.size);
			$(dialog).find( "#stationName" ).autocomplete({
				source: Array.from(availableStation)
			});
			$(dialog).find( "#deviceId" ).autocomplete({
				source: Array.from(availableDevice)
			});*/

			var condition = {};
			//初始化pagination
			$(dialog).find("#pagination1").jqPaginator({
				totalPages : 1,
				visiblePages : 10,
				currentPage : 1,
				first: '<li class="first"><a href="javascript:void(0);">首页<\/a><\/li>',
		        prev: '<li class="prev"><a href="javascript:void(0);">上一页<\/a><\/li>',
		        next: '<li class="next"><a href="javascript:void(0);">下一页<\/a><\/li>',
		        last: '<li class="last"><a href="javascript:void(0);">末页<\/a><\/li>',
		        page: '<li class="page"><a href="javascript:void(0);">{{page}}<\/a><\/li>',
				onPageChange : function(num, type) {
					condition["page"] = num;
					changePageRecord(dialog, condition);
				}
			});

			//定义“查询”按钮点击事件
			$(dialog).find("#detailsBtn").click(function() {
				var stationName = $(dialog).find("#stationName").val();
			   	var deviceId = $(dialog).find("#deviceId").val();
			   	var deviceType = $(dialog).find("#deviceType option:selected").text();
			   	var failureType = $(dialog).find("#failureType option:selected").text();
			   	var startTime = $(dialog).find(".calendartxt:eq(0)").val();
			   	var endTime = $(dialog).find(".calendartxt:eq(1)").val();
			   	
//			   	if(startTime>endTime) {
//			   		alert("请检查时间区间填写是否正确！");
//			   		return;
//			   	}
			   	
			   	condition = {
	   					"page" : 1,
				   		"stationName" : stationName,
						"deviceId" : deviceId,
						"deviceType" : deviceType,
						"failureType" : failureType,
						"startTime" : startTime,
						"endTime" : endTime
				 };
			   	changePageRecord(dialog, condition);
			   	$(dialog).find("#pagination1").jqPaginator('option', {
			   		currentPage : 1
			   	});
			   	
			});

		});
		
		
		
	});
   //设置定时1分钟一次
   timer=window.setInterval("failureRank()",1000*60);
});

//让指定的DIV始终显示在屏幕正中间  
function setCenter(dialog){   
	var height = $(window).height();
	var width = $(window).width();
	var scrollTop = $(window).scrollTop();   
    var scrollLeft = $(window).scrollLeft();   
    scrollTop = !scrollTop ? 0 : scrollTop;
    scrollLeft =!scrollLeft ? 0 : scrollLeft;
    var top = (height - dialog.height()) / 2 + scrollTop;
    var left =  (width - dialog.width()) / 2 + scrollLeft;
    dialog.attr('style','position:absolute;top:'+top+'px;left:'+left+'px;').show();  
}  

function openDialog(el, callback) {
	if (window.dialogObj) {
		window.dialogObj.remove();
	}
	window.dialogObj = $(el).clone().appendTo(document.body);
	callback(dialogObj);
	window.dialogObj.position({my: "center", at: "center", of: window}).show();
	window.dialogObj.find("span.closeable").click(function(){
		window.dialogObj.remove();
	});
	return window.dialogObj;
}


//ajax查询
function changePageRecord(dialog, condition){
	$.ajax({
		type : "GET",
		url : "/details/rtRecording.json",
		data : condition,
		success : function(data) {
			var html = template("recordDetail", {records : data.data});
			$(dialog).find(".tableContainer").html(html);
			$(dialog).find("#pagination1").jqPaginator('option', {
				totalPages : data.totalPages==0?1:data.totalPages
			});
			setCenter($(dialog));
			dialog.draggable();
		}
	});
}
//故障排行
function failureRank(){
	$.ajax({
		type : "GET",
		url : "/details/getFailureRank.json",
		data : {cityCode:'4401'},
		success : function(data) {
			 var data_html = "<tr><th>次数</th><th>站点</th><th>设备编号</th></tr>";
			   for (i in data) {
				   data_html += "<tr><td><b class='redfont'>" + data[i].failure_times
						   + "</b></td><td>" + data[i].station_name
						   + "</td><td>" + data[i].device_id
						   + "</td></tr>";
			   }
			$("#failure_rank").html(data_html);
		}
	});
}
