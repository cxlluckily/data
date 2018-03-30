package com.shankephone.data.common.web.socket;

import java.io.Serializable;

public class SubInfo implements Serializable {

	private static final long serialVersionUID = -2160795271072509361L;
	
	private boolean pattern = false;
	
	private String topic; 
	
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SubInfo other = (SubInfo) obj;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}

}
