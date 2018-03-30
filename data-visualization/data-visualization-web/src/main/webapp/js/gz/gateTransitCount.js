var GateTransitCount = function(opt) {
	
	this.topic = "gate:vol:4401";
	
	this.pattern = false;
	
	this.handle = function(data) {
		/*if (console) {
			console.log(JSON.stringify(data));
		}*/
		$("[gateTransit]").each(function(i){
			var statValue = data[$(this).attr("gateTransit")];
			$(this).html(statValue);
		});
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}