var TicketRecord = function(opt) {
	
	this.topic = "ticket:record:4401";
	
	this.pattern = false;
	
	this.handle = function(data) {
		var oldTime = $("#realtime").text();
		var str = '<tr><td>' + data.cityName + '</td><td>' + 
		data.entryStation+ '~' + data.exitStation + '</td><td>' + 
		data.ticketType + '</td><td>' + data.ticketNum + '张</td></tr>\
		<tr><td colspan="4" class="ge"></td></tr>';
		
		$("#ticketRecord").prepend(str);
		if($("#ticketRecord tr").length>6){
			$("#ticketRecord tr:gt(5)").remove();
		}
		
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}