var chartActive = null;
var UserActive = function(opt) {
	
	this.topic = "user:active:4401";
	
	this.pattern = false;
	
	this.el = null;
	
	this.handle = function(data) {
		if(chartActive==null){
			chartActive = echarts.init(document.getElementById('mainActive'));
		}
		optionActive.xAxis[0].data = data.xaxis;
		optionActive.series[0].data = data.news;
		optionActive.series[1].data = data.olds;
		chartActive.setOption(optionActive);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}
//近30日每日新老用户
optionActive = {
	    title: {
	        text: '',
	    },
	    tooltip: {
	        trigger: 'axis',
	        axisPointer: {
	            type: 'cross',
	            crossStyle: {
	                color: '#3398DB'
	            }
	        }
	    },
	    legend: {
	        data:['新用户','老用户'],
	        itemWidth:40,
	        itemHeight:20,
	        textStyle:{
	            fontSize:18,//图表文字颜色跟大小
	            color:'#fff'
	        }
	   	   },
	      grid: {
	       left: '0',
	       right: '30',
	       bottom: '0',
	       containLabel: true
	   },
	  xAxis: [
	  	{
	          type: 'category',
				data:[],
	          axisPointer: {
	              type: 'shadow'
	          },
	          axisLine: {
	              lineStyle: {
	                  color:'#3398DB',
	                  width:'2'//坐标轴颜色
	              }
	          },
	          axisLabel: {
                  color: '#fff'//坐标轴颜色 //文字颜色
	          }
	      }
		 ],
		yAxis: [
				{
			     type: 'value',
			     axisLine: {
			         lineStyle: {
			             type: 'solid',
			             color:'#539eb3',
			             width:'2'//坐标轴颜色
			         }
			     },
			     axisLabel: {
		             color: '#fff'
			     }
				}
			],
	  series: [
	        {
	            name:'新用户',
	            type:'line',
	            itemStyle:{
		            normal:{
		                color: "#fcee06" //折线点颜色
		            	}
	        	},
	        	lineStyle:{
		            normal:{
		                width:4,  //连线粗细
		                color: "#fcee06"  //折线条连线颜色
		            	}
	        	},
	        	yAxisIndex: 0,
				data:[]
	       },
	       {
	            name:'老用户',
	            type:'line',
	            itemStyle:{
		            normal:{
		                color: "#0ceecf" //折线点颜色
		            }
	        	},
	        	lineStyle:{
	            	normal:{
	            		width:4,  //连线粗细
	                	color: "#0ceecf"  //折线条连线颜色
	            	}
	        	},
	            yAxisIndex: 0,
				data:[]
	        }   
	      ]
     };
