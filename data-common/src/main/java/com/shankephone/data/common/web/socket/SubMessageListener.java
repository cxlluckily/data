package com.shankephone.data.common.web.socket;

import org.redisson.api.listener.MessageListener;
import org.redisson.api.listener.PatternMessageListener;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class SubMessageListener implements MessageListener<String>, PatternMessageListener<String> {
	
	private WebSocketSession session;
	
	public SubMessageListener(WebSocketSession session) {
		this.session = session;
	}
	
	@Override
	public void onMessage(String pattern, String channel, String msg) {
		if (!session.isOpen()) {
			return;
		}
		try {
			SubMessage subMessage = new SubMessage();
			subMessage.setTopic(pattern != null ? pattern : channel);
			subMessage.setPattern(pattern != null);
			subMessage.setData(JSON.parse(msg));
			synchronized (session) {
				session.sendMessage(new TextMessage(JSONObject.toJSONString(subMessage)));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onMessage(String channel, String msg) {
		onMessage(null, channel, msg);
	}
	
}
