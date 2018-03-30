var Sub = {};

Sub.url = "/socket/sub";

Sub.retryDelay = 3000;

Sub.isopen = false;

Sub.subMap = {};

Sub.subMapTemp = {};

Sub.subscribeBuffer = new Array();

Sub.subscribe = function(obj) {
	if (!Sub.isopen) {
		Sub.subscribeBuffer.push(obj);
		return;
	}
	var subscriber = Sub.subMap[obj.topic];
	if (!subscriber) {
		subscriber = new Array();
		Sub.subMap[obj.topic] = subscriber;
	}
	subscriber.push(obj);
	var msg = {};
	msg.topic = obj.topic;
	msg.pattern = obj.pattern;
	Sub.socket.send(JSON3.stringify(msg));
}

Sub.createConnection = function() {
	var socket = new SockJS(Sub.url, null, {transports: ["websocket"]});
	Sub.socket = socket;
	socket.onopen = Sub.onopen;
	socket.onclose = Sub.onclose;
	socket.onerror =Sub.onerror;
	socket.onmessage = Sub.onmessage;
}

Sub.onopen = function() {
	Sub.isopen = true;
	for (var i in Sub.subscribeBuffer) {
		Sub.subscribe(Sub.subscribeBuffer[i]);
	}
	Sub.subscribeBuffer = new Array();
}

Sub.onclose = function(event) {
	if (console) {
		console.log("onclose [wasClean : " + event.wasClean + " , code : " + event.code + " , reason : " + event.reason + "]");
	}
	Sub.isopen = false;
	Sub.subscribeBuffer = new Array();
	if (!$.isEmptyObject(Sub.subMap)) {
		Sub.subMapTemp = Sub.subMap;
		Sub.subMap = {};
	}
	setTimeout(function(){
		Sub.createConnection();
		for (var i in Sub.subMapTemp) {
			for (var j in Sub.subMapTemp[i]) {
				Sub.subscribe(Sub.subMapTemp[i][j]);
			}
		}
	}, Sub.retryDelay);
}

Sub.onerror = function(event) {
	if (console) {
		console.log("event [message : " + event.message + " , filename : " + event.filename + " , lineno : " + event.lineno + " , colno : " + event.colno + " , error : " + event.error + "]");
	}
}

Sub.onmessage = function(event) {
	var msgData = JSON3.parse(event.data);
	var subscriber = Sub.subMap[msgData.topic];
	for (var i in subscriber) {
		subscriber[i].handle(msgData.data);
	}
}

Sub.createConnection();
