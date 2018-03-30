package com.shankephone.data.monitoring.util;

import java.util.HashMap;
import java.util.Map;

public class Constants {
	
	public static Map<String,String> failureTypeMap = new HashMap<String,String>();
	static {
		failureTypeMap.put("1", "停止运营");
		failureTypeMap.put("2", "维修模式");
		failureTypeMap.put("3", "故障模式");
		failureTypeMap.put("4", "通讯中断");
		failureTypeMap.put("9", "离线模式");
	}
	
	/**
	 * 故障模式：1-停止运营，2-维修模式，3-故障模式，4-通讯中断
	 */
	public static final String FAILURE_MODE_STOP = "1";
	public static final String FAILURE_MODE_REPAIR = "2";
	public static final String FAILURE_MODE_FAILURE = "3";
	public static final String FAILURE_MODE_INTERRUPT = "4";
	
	//离线状态
	public static final String FAILURE_STATUS_OFFLINE = "9";
	
	/**
	 * 故障类型
	 */
	public static final String FAILURE_TYPE_FAILURE = "0";
	public static final String FAILURE_TYPE_OFFLINE = "1";

}
