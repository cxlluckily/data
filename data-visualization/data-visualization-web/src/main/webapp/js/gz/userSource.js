var TradeSourceAll = function(opt) {
	this.topic = "user:source:vol:4401";
	this.pattern = false;
	this.handle = function(data) {
		/*if (console) {
			console.log(JSON.stringify(data));
		}*/
		$("[tradeSource]").each(function(i){
			var statValue = data[$(this).attr("tradeSource")];
			$(this).html(statValue);
		});
	}
	for (var i in opt) {
		this[i] = opt[i];
	}
	Sub.subscribe(this);
}
