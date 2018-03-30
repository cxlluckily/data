var ticketPayment = null;
var PayType = function(opt) {
	
	this.topic = "pay:type:4401";
	
	this.pattern = false;
	
	this.handle = function(data) {
		/*if (console) {
			console.log(JSON.stringify(data));
		}*/
		
		var names=new Array();
		var values=new Array();
		var nameFlag="";
		$.each(data,function(name,value) {
			if(name=="zfb"){
				nameFlag="支付宝";
			}else if(name=="wx"){
				nameFlag="微信";
			}else if(name=="zyd"){
				nameFlag="中移动";
			}else if(name=="yzf"){
				nameFlag="翼支付";
			}else if(name=="sxyzf"){
				nameFlag="首信易支付";
			}else if(name=="yl"){
				nameFlag="银联";
			}else if(name=="other"){
				nameFlag="其它";
			}
			var obj={'name':nameFlag,'value':value};
			values.push(obj);
		});
		values.sort(compare);
		for(var i=0;i<values.length;i++){
			names.push(values[i].name);
		}
		if(ticketPayment == null){
			ticketPayment = echarts.init(document.getElementById('mainTicketPayment'));
		}
		optionTicketPayment.legend.data = names;
		optionTicketPayment.series[0].data = values;
		ticketPayment.setOption(optionTicketPayment);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}
optionTicketPayment = {
	    tooltip : {
	        trigger: 'item',
	        formatter: "{b} : {c} ({d}%)"
	    },
	    legend: {
	        orient:'vertical',
	        left: '40px',
			top:'60px',
			textStyle:{
		            fontSize:15,
		            color:'#fff'
		    },
	        data:['支付宝','微信']
	    },
	    
	    calculable : true,
	    series : [
	        {
	            type:'pie',
	            color: ['#33FF00','#f3fb09','#10cefe','#FFCC66','#FFFFF4','#fc9706',	'#FFBD9D'],
	            radius : [40, 70],
	            center: ['65%', '50%'],
	            roseType : 'radius',
	            labelLine:{
	              normal:{
	                  length:30,
	                  length2:30
	              }  
	            },
	            data:[]
	        },
	        {
	            type:'pie',
	            radius : [80, 81],
	            roseType : 'radius',
	            center: ['65%', '50%'],
	            color: ['#f3fb09'],
	            silent:true,
	            label: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            lableLine: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            data:[
	                {value:100, name:'边1'}
	            ]
	        },
	        {
	            type:'pie',
	            color: ['#87CEEB'],
	            radius : [88, 89],
	            center: ['65%', '50%'],
	            roseType : 'radius',
	            silent:true,
	            label: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            lableLine: {
	                normal: {
	                    show: false
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            data:[
	                {value:100, name:'边2'}
	            ]
	        }
	    ]
};

var compare = function (obj1, obj2) {
    var val1 = obj1.value;
    var val2 = obj2.value;
    if (val1 < val2) {
        return 1;
    } else if (val1 > val2) {
        return -1;
    } else {
        return 0;
    }            
} 