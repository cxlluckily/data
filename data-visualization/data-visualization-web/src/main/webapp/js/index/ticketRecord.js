var TicketRecord = function(opt) {
	
	this.topic = "ticket:record:0000";
	
	this.pattern = false;
	
	this.handle = function(data) {
		
		var entry = data.entryStation ? data.entryStation : ""; 
		var exit = data.exitStation ? data.exitStation : ""; 
		var stations;
		if(entry == ""){
			if(exit == ""){
				//console.log("----------没有站点！");
			} else {
				stations = exit;
			}
		} else {
			if(exit == ""){
				stations = entry;
			} else {
				stations = entry + '~' + exit;
			}
		}
		var typeFlag=data.type;
		var unitFlag="张";
		if(typeFlag=="coffee"||typeFlag=="nfc"){
			unitFlag="笔";
		}
		if(stations == undefined || !stations){
			//console.log("异常-------" + JSON.stringify(data));
			stations = "---";
		}
		
		var str = '<tr><td width="60">&nbsp;&nbsp;' + data.cityName + '</td><td width="238">' + 
		stations + '</td><td width="110">' + 
		data.ticketType + '</td><td width="60">' + data.ticketNum + unitFlag +'&nbsp;&nbsp;</td></tr>\
		<tr><td colspan="4" class="ge"></td></tr>';
		
		$("#ticketRecord").prepend(str);
		if($("#ticketRecord tr").length>12){
			$("#ticketRecord tr:gt(11)").remove();
		}
		var i=0;
		$("#ticketRecord tr").each(function(){ 
			if(i==0){
				$(this).find("td").addClass("j");	
			}else if(i==4){
				$(this).find("td").addClass("j");	
			}else if(i==8){
				$(this).find("td").addClass("j");	
			}else{
				$(this).find("td").removeClass("j");	
			}
			i++;
		});
	}
	
	for (var i in opt) {
		this[i] = opt[i];
	}
	
	Sub.subscribe(this);
}