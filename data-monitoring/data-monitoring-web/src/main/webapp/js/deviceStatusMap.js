var DeviceStatusMap = function(opt) {

	var _this = this;
	this.topic = "device_failure_station_4401";
	this.pattern = false;
	this.width = 1920,
	this.height = 1080,
	this.data = [];
	this.filter = {}
	this.statusPriority = [
	   {name : "device_offline_num", symbol : "image://images/g_icon1.png"},
	   {name : "device_failure_num", symbol : "image://images/g_icon2.png"},
	   {name : "device_interrupt_num", symbol : "image://images/g_icon3.png"},
	   {name : "device_repair_num", symbol : "image://images/g_icon4.png"},
	   {name : "device_stop_num", symbol : "image://images/g_icon5.png"}
	];
	
	this.handle = function(data) {
		if (!data || !data.station) {
			return;
		}
		_this.data = data;
		this.handleChart();
		this.handleFilter();
	}

	this.handleChart = function() {
		var data = _this.data;
		var dataCountArray = [];
		var dataImgArray = [];
		for (var i in data.station) {
			var station = data.station[i];
			var statusInfo = _this.getStatusInfo(station);
			if (!statusInfo) {
				continue;
			}
			var dataCount = {};
			var dataImg = {};
			for (var im in stationGz) {
				if (stationGz[im].name == station.station_name){
					dataCount.value = [];
					dataCount.value[0] = stationGz[im].value[0];
					dataCount.value[1] = stationGz[im].value[1];
					dataImg.value = [];
					dataImg.value[0] = stationGz[im].value[0];
					dataImg.value[1] = stationGz[im].value[1];
					break;
				}
			}
			if (!dataCount.value) {
				//console.error("站点：" + station.station_name + "，尚未配置相应坐标！");
				continue;
			}
			dataCount.code = station.station_id;
			dataCount.name = station.station_name;
			dataCount.value[1] = _this.height - dataCount.value[1];
			dataCount.value[2] = statusInfo.count;
			dataCountArray.push(dataCount);

			dataImg.code = station.station_id;
			dataImg.name = station.station_name;
			dataImg.value[1] = _this.height - dataImg.value[1];
			dataImg.symbol = statusInfo.symbol;
			dataImgArray.push(dataImg);
		}
		//console.log("dataCount:" + JSON.stringify(dataCountArray));
		//console.log("dataImg:" + JSON.stringify(dataImgArray));
		_this.refreshChart(dataCountArray, dataImgArray);
	}

	this.handleFilter = function() {
		 let data = _this.data;
		 //console.log(data);
		 $(".totalStatus h2 span").each(function(i){
			let status = $(this).attr("status"); 
			if (data && data[status]) {
				$(this).text(data[status]);
			} else {
				$(this).text(0);
			}
		 });
		 var countProperty = _this.getDeviceCountProperty();
		 $(".status_table td").each(function(){
			let status = $(this).attr("status"); 
			let type = $(this).attr("type");
			if (type == "status") {
				if (data[status] && data[status][countProperty]) {
					$(this).find("h2 span").text(data[status][countProperty]);
				} else {
					$(this).find("h2 span").text(0);
				}
			}
		 });
		 var deviceCount = _this.getDeviceCount(data);
		 $(".status_table td[status=gate_num] h2 span").text(deviceCount.gateCount);
		 $(".status_table td[status=ticket_num] h2 span").text(deviceCount.ticketCount);
	}
	
	this.getDeviceCount = function(data) {
		let ticketCount = 0; 
		let gateCount = 0; 
		for (let status of _this.statusPriority) {
			if (_this.filter[status.name]) {
				continue;
			}
			if (!data[status.name]) {
				continue;
			}
			let ticketNum = data[status.name].ticket_num || 0;
			let gateNum = data[status.name].gate_num || 0;
			let numZj = 0;
			let numGpj = 0;
			if(status.name=="device_failure_num"){
				numZj = data["device_offline_num"].ZJ_failureRepeat;
				numGpj = data["device_offline_num"].GPJ_failureRepeat;
			}
			if(status.name=="device_interrupt_num"){
				numZj = data["device_offline_num"].ZJ_interruptRepeat;
				numGpj = data["device_offline_num"].GPJ_interruptRepeat;
			}
			if(status.name=="device_repair_num"){
				numZj = data["device_offline_num"].ZJ_repairRepeat;
				numGpj = data["device_offline_num"].GPJ_repairRepeat;
			}
			if(status.name=="device_stop_num"){
				numZj = data["device_offline_num"].ZJ_stopRepeat;
				numGpj = data["device_offline_num"].GPJ_stopRepeat;
			}
			ticketCount += ticketNum-numGpj;
			gateCount += gateNum-numZj;
		}
		return { ticketCount : ticketCount, gateCount : gateCount};
	}
	
	this.getStatusInfo =  (data) => {
		let symbol = null; 
		let count = 0; 
		for (let status of _this.statusPriority) {
			if (_this.filter[status.name]) {
				continue;
			}
			if (!data[status.name]) {
				continue;
			}
			let numProperty = _this.getDeviceCountProperty();
			let num = data[status.name][numProperty];
			if (!num || num == 0) {
				continue;
			}
			count += num;
			if (symbol == null) {
				symbol = status.symbol;
			}
		}
		if (count == 0) {
			return null;
		}
		return {symbol : symbol, count : count};
	}
	
	this.getDeviceCountProperty = function() {
		if (_this.filter.gate_num === true && _this.filter.ticket_num === true) {
			return null;
		} else if (_this.filter.gate_num === true) {
			return "ticket_num";
		} else if (_this.filter.ticket_num === true) {
			return "gate_num";
		} else {
			return "total_num";
		}
	}
	
	function sortChartData(o1, o2){
		if (o1.name < o2.name) {
			return -1;
		} else if (o1.name > o2.name) {
			return 1;
		} else {
			return 0;
		}
	}
	
	function isChartDataEquals(dataArray1, dataArray2) {
		if (!dataArray1 || !dataArray2) {
			return false;
		}
		if (dataArray1.length != dataArray2.length) {
			return false;
		}
		for (var i in dataArray1) {
			if (dataArray1[i].name != dataArray2[i].name) {
				return false;
			}
			for (var j in dataArray1[i].value) {
				if (dataArray1[i].value[j] != dataArray2[i].value[j]) {
					return false;
				}
			}
			if (dataArray1[i].symbol && dataArray2[i].symbol
					&& dataArray1[i].symbol != dataArray2[i].symbol) {
				return false;
			}
		}
		return true;
	}
	
	this.refreshChart = function (dataCountArray, dataImgArray) {
		dataCountArray.sort(sortChartData);
		dataImgArray.sort(sortChartData);
		if (isChartDataEquals(_this.dataCountArray, dataCountArray)
		    && isChartDataEquals(_this.dataImgArray, dataImgArray)){
			return;
		}
		_this.dataCountArray = dataCountArray;
		_this.dataImgArray = dataImgArray;
		_this.chart.setOption({
	        series: [{
	            data: dataCountArray
	        },{
	            data: dataImgArray
	        }]
	    });
	}

	this.loadStationDetail = function (data) {
		var stationName = data.name;
		$.ajax({
			type : "GET",
			url : "/details/station.json",
			data : {
				"cityCode" : 4401,
				"stationName" : stationName
			},
			success : function(data) {
				openDialog(".stationDialog", function(dialog){
					$(dialog).find(".title").text(data.stationName + "-" + data.lineName);
					$(dialog).find("li").click(function(){
						$(dialog).find(".tabon").removeClass("tabon");
						$(this).addClass("tabon");
						let type = $(this).attr("type");
						let html = template("stationDetail", { machine : data[type]});
						$(dialog).find(".tableContainer").html(html);
					});
					$(dialog).find("li:first").click();
				});
			}
		});	
	}
	
	this.initChart = function () {
		var chartDom = document.getElementById('subway');
		chartDom.style.left = '0px';
		chartDom.style.top = '0px'
		chartDom.style.position = 'absolute';
		chartDom.style.width = _this.width + 'px';
		chartDom.style.height = _this.height + 'px';
		_this.chart = echarts.init(chartDom);
		var option = {
			  grid: {
		          left: 0,
		          top: 0,
		          right: 0,
		          bottom: 0,
		      },
		      xAxis: {
		          nameGap: 0,
		          show: false,
		          type: 'value',
		          min: 0,
		          max: _this.width
		      },
		      yAxis: {
		          nameGap: 0,
		          show: false,
		          type: 'value',
		          min: 0,
		          max: _this.height,
		      },
		      tooltip:{
		        show : false
		      },
		      series: [{
		          type : 'scatter',
		          coordinateSystem : 'cartesian2d',
		          symbol : 'pin',
		          symbolSize : 30,
		          label : {
		              normal : {
		                  show : true,
		                  textStyle : {
		                      color : '#fff',
		                      fontSize : 16,
		                      fontWeight : 'bold'
		                  }
		              }
		          },
		          itemStyle : {
		              normal : {
		                  color: '#F62157',
		              }
		          },
		          zlevel : 6,
		          data : []
		      },{
		    	  type : "effectScatter",
		          coordinateSystem : "cartesian2d",
		          symbolSize : 30,
	              hoverAnimation : true,
	              zlevel : 2,
	              data : []
	          }]
		};
		_this.chart.setOption(option);
		_this.chart.on('click', function (params) {
			_this.loadStationDetail(params.data);
		});		
		Sub.subscribe(_this);
	}

	this.initFilter = function() {
		$(".status_table td").click(function(){
			let statusImg = $(this).find("div img");
			let status = $(this).attr("status");
			statusImg.toggleClass("gray");
			_this.filter[status] = statusImg.hasClass("gray");
			_this.handle(_this.data);
		});
		$(".status_table td").each(function(){
			let status = $(this).attr("status");
			let statusImg = $(this).find("div img");
			_this.filter[status] = statusImg.hasClass("gray");	
		});
	}
	for (var i in opt) {
		this[i] = opt[i];
	}
	_this.initChart();
	_this.initFilter();
}