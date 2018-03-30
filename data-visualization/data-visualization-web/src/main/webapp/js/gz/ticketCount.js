var chartTicket = null;
var TicketCount = function(opt) {
	
	this.topic = "day:tickets:4401";
	
	this.pattern = false;
	
	this.el = null;
	
	this.handle = function(data) {
		if(chartTicket == null){
			chartTicket = echarts.init(document.getElementById('mainTicket'));
		}
		optionTicket.xAxis[0].data = data.TIME;
        optionTicket.series[0].data = data.TICKETS;
        chartTicket.setOption(optionTicket);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}
////近30日售票情况
optionTicket = {
		color: ['#3398DB'],
		tooltip:{//提示框，鼠标悬浮交互时的信息提示。
			trigger:'axis',
			axisPointer:{//坐标轴指示器，默认type为line，可选为：'line' | 'cross' | 'shadow' | 'none'(无)
				type : 'shadow'
			}
		},
		grid:{
			left: '5',
	        right: '5',
	        bottom: '5',
	        top:'30',
	        containLabel: true
		},
	    xAxis : [
        {
            type : 'category',
            // data : ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            axisLine: {
                lineStyle: {
                    type: 'solid',
                    color:'#539eb3',
                    width:'2'//坐标轴颜色
                }
            },
            axisLabel: {
                color: '#fff'//坐标轴颜色 //文字颜色
            },
            data:[],
            axisTick: {
                alignWithLabel: true
            }
        }
    ],
    yAxis : [
		{
		  type : 'value',
		  axisLine: {
		      lineStyle: {
		          type: 'solid',
		          color:'#539eb3',
		          width:'2'//坐标轴颜色
		      }
		  },
		  axisLabel: {
	          color: '#ffffff'
		  },
		}
		],
		
		series : [
		{
		  name:'',
		  type: 'bar',
		  itemStyle: {
		      normal: {
		          color: new echarts.graphic.LinearGradient(
		              0, 0, 0, 1,
		              [
		                  {offset: 0, color: '#83bff6'},
		                  {offset: 0.5, color: '#188df0'},
		                  {offset: 1, color: '#188df0'}
		              ]
		          )
		      },
		      emphasis: {
		          color: new echarts.graphic.LinearGradient(
		              0, 0, 0, 1,
		              [
		                  {offset: 0, color: '#2378f7'},
		                  {offset: 0.7, color: '#2378f7'},
		                  {offset: 1, color: '#83bff6'}
		              ]
		          )
		      }
		  },
		  data:[]
		}
	]
};