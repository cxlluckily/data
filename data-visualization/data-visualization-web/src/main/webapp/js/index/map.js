$(function(){
	var data = [
		     {name: '南宁', value: 220},
		     {name: '广州', value: 300},
		     {name: '青岛', value: 200},
		     {name: '长沙', value: 110},
		     {name: '西安', value: 150},
		     {name: '郑州', value: 100},
		     {name: '珠海', value: 100},
		     {name: '深圳', value: 80},
		     {name: '成都', value: 80},
		     {name: '无锡', value: 80},
		     {name: '杭州', value: 80},
		     {name: '哈尔滨', value:80},
		     {name: '福州', value: 80},
		     {name: '贵阳', value: 80}
		];
       var geoCoordMap = {
			    '南宁':[108.33,22.84],
			    '广州':[113.23,23.16],
			    '青岛':[120.33,36.07],
			    '长沙':[113,28.21],
			    '西安':[108.95,34.27],
			    '郑州':[113.65,34.76],
			    '珠海':[113.52,22.3],
				'深圳':[114.07,22.62],
			    '成都':[104.06,30.67],
			    '无锡':[120.29,31.59],
			    '杭州':[120.19,30.26],
			    '哈尔滨':[126.63,45.75],
			    '福州':[119.3,26.08],
			    '贵阳':[106.636576,26.653226]
			}
		var convertData = function(data) {
			var res = [];
			for(var i = 0; i < data.length; i++) {
				var geoCoord = geoCoordMap[data[i].name];
				if(geoCoord) {
					res.push({
						name: data[i].name,
						value: geoCoord.concat(data[i].value)
					});
				}
			}
			return res;
		};

		var optionMap = {
			geo: {
				top : -20,
				map: 'china',
				label: {
					emphasis: {
						show: false
					}
				},
				roam: true,
				zoom:0.93,
				itemStyle: {
					normal: { //地图默认状态下每个省的设置
						areaColor: '#073768', //区域颜色
						borderColor: '#51A6F2' //边框颜色
					},
					emphasis: { //地图激活状态下每个省的设置，hover态
						areaColor: '#0E6FD1'
					}
				}
			},
	      series : [
	        {
	            type: 'scatter',
	            coordinateSystem: 'geo',
	            data: convertData(data),
	            symbolSize: function (val) {
	                return val[2] / 10;
	            },
	            label: {
	                normal: {
	                    formatter: '{b}',
	                    position: 'right',
	                    show: true
	                },
	                emphasis: {
	                    show: true
	                }
	            },
	            itemStyle: {
	                normal: {
	                    color: '#ddb926'
	                }
	            }
	        },
	        {
	            type: 'effectScatter',
	            coordinateSystem: 'geo',
	            data: convertData(data.sort(function (a, b) {
	                return b.value - a.value;
	            }).slice(0, 7)),
	            symbolSize: function (val) {
	                return val[2] / 10;
	            },
	            showEffectOn: 'render',
	            rippleEffect: {
	                brushType: 'stroke'
	            },
	            hoverAnimation: true,
	            label: {
	                normal: {
	                    formatter: '{b}',
	                    position: 'right',
	                    show: true
	                }
	            },
	            itemStyle: {
	                normal: {
	                    color: '#f4e925',
	                    shadowBlur: 10,
	                    shadowColor: '#333'
	                }
	            },
	            zlevel: 1
	        }
	    ]
     };
	var skpMap = document.getElementById("skpMap");
	var myChartMap = echarts.init(skpMap);
	myChartMap.setOption(optionMap);
});