var LastTimeSub = function(opt) {
	
	this.topic = "ticket:lastTime";
	
	this.pattern = false;
	
	this.handle = function(data) {
		/*if (console) {
			console.log(JSON.stringify(data));
		}*/
		var dataAll = data.all;
		var lastTime = data.lastTime;
		var date = lastTime.split(' ')[0];
		var time = lastTime.split(' ')[1];
		var timeEl = $("#realtime");
		timeEl.html(date + '&nbsp;&nbsp;&nbsp;&nbsp;' + time);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}