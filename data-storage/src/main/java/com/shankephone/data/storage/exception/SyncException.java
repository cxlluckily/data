package com.shankephone.data.storage.exception;

import com.alibaba.fastjson.JSONObject;

public class SyncException  extends HandleException {
	
	private static final long serialVersionUID = 1L;
	
	private final static String prompt = "【同步处理异常】";
	
	public SyncException(String message){
		super(prompt + " : " + message);
	}
	
	public SyncException(Exception e){
		super(e);
	}
	
	public SyncException(Exception e, String message){
		super(prompt + ":" + message, e);
	}
	
	public SyncException(Exception e, String message, JSONObject json){
		super(prompt + ":" + message, e, json);
	}
	
	public SyncException(Exception e, JSONObject json){
		super(e, json);
	}
	
	public SyncException(String message, JSONObject json){
		super(prompt + " : " + message, json);
	}
	
	
}