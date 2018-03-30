package com.shankephone.data.storage.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;

public class HandleException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(HandleException.class);
	private static final String tip = "【错误数据】：";
	private static boolean print = true; 
	
	public HandleException(Exception e){
		super(e);
		if(print){
			e.printStackTrace();
		}
	}
	
	public HandleException(String message){
		super(message);
		logger.error(message);
	}
	
	public HandleException(String message, JSONObject json){
		super(message);
		logger.error(message);
		logger.error(tip + json.toJSONString());
	}
	
	public HandleException(String message, Exception e){
		super(message, e);
		logger.error(message);
		if(print){
			e.printStackTrace();
		}
	}
	
	public HandleException(String message, Exception e, JSONObject json){
		super(message, e);
		logger.error(tip + "\r\n" + json.toJSONString());
		if(print){
			e.printStackTrace();
		}
	}
	
	public HandleException(Exception e, JSONObject json){
		super(json.toJSONString(), e);
		logger.error(tip + "\r\n" + json.toJSONString());
		if(print){
			e.printStackTrace();
		}
	}
	
}
