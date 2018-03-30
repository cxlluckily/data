var UserCount = function(opt) {
	
	this.topic = "user:uniq:4401";
	
	this.pattern = false;
	
	this.handle = function(data) {
		/*if (console) {
			console.log(JSON.stringify(data));
		}*/
		var userCount = data.USER_COUNT;
		$('#userCount').html(userCount);
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}