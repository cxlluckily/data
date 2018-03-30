package com.shankephone.data.common.kafka;

public enum KafkaOffset {
	
	START("start"),
	END("end");
	
	private String name;
	
	private KafkaOffset(String name) {
		this.name = name;
	}
	
	public String value() {
		return this.name;
	}
	
}