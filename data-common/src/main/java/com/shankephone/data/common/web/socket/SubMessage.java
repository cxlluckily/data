package com.shankephone.data.common.web.socket;

import java.io.Serializable;

public class SubMessage implements Serializable {

	private static final long serialVersionUID = 1731907019704157584L;

	private boolean pattern = false;
	
	private boolean init = false;
	
	private String topic; 
	
	private Object data;
	
	public boolean isPattern() {
		return pattern;
	}

	public void setPattern(boolean pattern) {
		this.pattern = pattern;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Object getData() {
		return data;
	}

	public void setData(Object data) {
		this.data = data;
	}

	public boolean isInit() {
		return init;
	}

	public void setInit(boolean init) {
		this.init = init;
	}

}
