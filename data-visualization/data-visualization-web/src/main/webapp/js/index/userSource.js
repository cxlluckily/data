var chartSource = null;
var TradeSourceAll = function(opt) {
	
	this.topic = "user:source:vol:0000";
	
	this.pattern = false;
	
	this.handle = function(data) {
		/*if (console) {
			console.log(JSON.stringify(data));
		}*/
		$("[tradeSource]").each(function(i){
			var statValue = data[$(this).attr("tradeSource")];
			$(this).html(statValue);
		});
		var names=new Array();
		var values=new Array();
		var nameFlag="";
		$.each(data,function(name,value) {
			if(name=="skf"){
				nameFlag="闪客蜂APP";
			}else if(name=="wx"){
				nameFlag="微信小程序";
			}else if(name=="zfb"){
				nameFlag="支付宝城市服务";
			}else if(name=="ygpj"){
				nameFlag="云购票机";
			}else if(name=="gzdt"){
				nameFlag="广州地铁APP";
			}else if(name=="hb"){
				nameFlag="和包APP";
			}else if(name=="qt"){
				nameFlag="其它";
			}
			var obj={'name':nameFlag,'value':value};
			values.push(obj);
		});
		values.sort(compare);
		for(var i=0;i<values.length;i++){
			names.push(values[i].name);
		}
		
		if(chartSource==null){
			chartSource = echarts.init(document.getElementById('mainSource'));
		}
		optionSource.legend.data = names;
		optionSource.series[0].data = values;
		chartSource.setOption(optionSource);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}
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
optionSource = {
	    tooltip: {
	        trigger: 'item',
	        formatter: "{b}: {c} ({d}%)"
		},
		//固定圆环颜色
		color:['#33FF00','#f3fb09','#10cefe','#FFCC66','#FFFFF4','#fc9706',	'#FFBD9D'],  
	    legend: {
	        orient: 'vertical',
	        x: 'left',
			data:[],
			itemWidth:30,
            itemHeight:20,
            textStyle:{
               fontSize:15,
               color:'#fff'
            }
	    },
	    series: [
	        {
	            name:'用户来源',
	            type:'pie',
	            center:['60%', '50%'],
				radius:['100%', '60%'],
				// 改动位置
				//  center: ['50%', '50%'],		 
	            avoidLabelOverlap: false,
	            label: {
	                normal: {
	                    show: false,
	                    position: 'center'
	                },
	                emphasis: {
	                    show: true,
	                    textStyle: {
	                        fontSize: '10',
	                        fontWeight: 'bold'
	                    }
	                }
	            },
	            labelLine: {
	                normal: {
	                    show: false
	                }
	            },
	            data:[
	                
	            ]
	        }
	    ]
	};