var TradeStatAll = function(opt) {
	
	this.topic = "trade:vol:4401";
	
	this.pattern = false;
	
	this.handle = function(data) {
		/*if (console) {
			console.log(JSON.stringify(data));
		}*/
		var tradeStatAll = data.all;
		var tradeStatAllHtml = $("#tradeStatAll p");
		var temp = tradeStatAll;
		for (var i= tradeStatAllHtml.length - 1; i >= 0; i--){
			tradeStatAllHtml.eq(i).html(temp % 10);
			temp = parseInt(temp / 10);
		}
		$("[tradeStat]").each(function(i){
			var statValue = data[$(this).attr("tradeStat")];
			$(this).html(statValue);
		});
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}