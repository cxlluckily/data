var supplementChart = null;
var SupplementTrans = function(opt) {
	
	this.topic = "trans:xxhf:supplement:4401";
	
	this.pattern = false;
	
	this.el = null;
	
	this.handle = function(data) {
		var timeSet = new Set();
		if(supplementChart==null){    //初始化
			supplementChart = echarts.init(document.getElementById('supplementChart'));
			
			for(var i = data.length - 1; i >= 0; i--) {

				for(var j = 0; j < data[i].time.length; j++) {
					timeSet.add(data[i].time[j]);
				}
				supplementOption.legend.data.push(data[i].day);				
				var item =  {
			            name : data[i].day,
			            type : 'line',
			            itemStyle : {
				            normal : {
				            	opacity : 0
				            }
			        	},
			        	lineStyle : {
			            	normal : {
			            		width : 2
			            	}
			        	},
						data:data[i].value
			        };

				if (data[i].today == true) {
					item.lineStyle.normal.width = 5;
				}
				supplementOption.series.push(item);
			}

			supplementOption.xAxis.data = Array.from(timeSet).sort();
			supplementChart.setOption(supplementOption);
		} else {
			
			var legendDays = supplementOption.legend.data;
			var curDayIndex = 0;
			for (; curDayIndex < legendDays.length; curDayIndex++) {
				if (data.day == legendDays[curDayIndex]) {
					break;
				}
			}
			if (curDayIndex == legendDays.length) {		//有新一天的数据
				if (legendDays.length == 3) {					//若当前已有三天的数据
					legendDays.shift();
					supplementOption.series.shift();  //删除第一天的数据
				}
				supplementOption.series[supplementOption.series.length - 1].lineStyle.normal.width = 2;
				//创建新一天的series
				var item =  {
			            name : data.day,
			            type : 'line',
			            itemStyle : {
				            normal : {
				            	opacity : 0
				            }
			        	},
			        	lineStyle : {
			            	normal : {
			            		width : 5
			            	}
			        	},
						data: []
			    };
				legendDays.push(data.day);
				supplementOption.series.push(item);
			}
			var newData = supplementOption.xAxis.data;

			newData.push(data.time);
			for(var j = 0; j < newData.length; j++) {
				timeSet.add(newData[j]);
			}

			supplementOption.xAxis.data = Array.from(timeSet).sort();
			supplementOption.series[supplementOption.series.length - 1].data.push(data.value);
			supplementChart.setOption(supplementOption);
		}
		
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}

supplementOption = {
		color: [ "#036BC8","#0CEECF","#FCEE06"],
	    tooltip: {
	        trigger: 'axis'
	    },
	    legend: {
	    	x : 'center',
	    	top: '5%',
	        data : [],
	        itemWidth : 40,
	        itemHeight : 0,
	        textStyle : {
	            fontSize : 12,//图表文字颜色跟大小
	            color : '#fff'
	        }
	   	},	      
	  xAxis: {
          type: 'category',
          data:[],
          axisPointer: {
              type: 'shadow'
          },
          axisLine: {
              lineStyle: {
                  color:'#fff',
                  width:'2'//坐标轴颜色
              }
          },
          axisLabel: {
              color: '#fff'//坐标轴颜色 //文字颜色
          }
	    },
		yAxis: {
			name: '补票',
			nameTextStyle: {
				fontSize: 16
			},
			type: 'value',
			splitLine: {show:false},
			axisLine: {
				lineStyle: {
					type: 'solid',
					color:'#fff',
					width:'2'//坐标轴颜色
				}
			},
			axisLabel: {
				color: '#fff'
			}
	   },
	  series: [] 
};
