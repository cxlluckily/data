package com.shankephone.data.common.web.socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import com.alibaba.fastjson.JSONObject;

public class SubHandler extends TextWebSocketHandler {
	
	private final Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public void afterConnectionEstablished(WebSocketSession session) throws Exception {
		logger.debug("建立连接，Session Id：" + session.getId());
	}

	@Override
	protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
		String msg = message.getPayload();
		logger.debug("收到客户端发送的消息：" + msg);
		SubInfo subInfo = JSONObject.parseObject(msg, SubInfo.class);
		SubHolder subHolder = SubHolder.getInstance(session);
		subHolder.sub(subInfo);
	}

	@Override
	public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
		logger.debug("关闭连接，Session Id：" + session.getId() + "，" + status);
		SubHolder subHolder = SubHolder.getInstance(session);
		subHolder.unsub();
	}
	
}
