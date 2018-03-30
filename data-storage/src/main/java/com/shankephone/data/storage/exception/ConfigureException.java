package com.shankephone.data.storage.exception;

public class ConfigureException extends HandleException {
	
	private static final long serialVersionUID = 1L;

	
	private final static String prompt = "【配置信息错误】";
	
	public ConfigureException(Exception e){
		super(e);
	}
	
	public ConfigureException(String message){
		super(prompt + " : " + message);
	}
	
	public ConfigureException(Exception e, String message){
		super(prompt + " : " + message, e);
	}
	
	
}
