package com.shankephone.data.monitoring.computing.util;

import java.util.HashMap;
import java.util.Map;

public class Constants {
	
	public static Map<String,String> cityMap = new HashMap<String,String>();
	static {
		cityMap.put("1200", "天津");
		cityMap.put("2660", "青岛");
		cityMap.put("4100", "长沙");
		cityMap.put("4401", "广州");
		cityMap.put("4500", "郑州");
		cityMap.put("5300", "南宁");
		cityMap.put("7100", "西安");
	}
	
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
	//设备正常状态值
	public static final String FAILURE_STATUS_NORMAL = "0";
	
	/**
	 * 故障类型
	 */
	public static final String FAILURE_TYPE_FAILURE = "0";
	public static final String FAILURE_TYPE_OFFLINE = "1";
	
	/**
	 * 心跳Key的过期时间
	 */
	public static final long HEART_EXPIRE_MINUTES = 3;
	
	/**
	 * 故障恢复的最小心跳次数
	 */
	public static final int FAILOVER_HEART_BEAT_TIMES = 3;
	
	/**
	 * 设备类型：01-购票机，02-闸机
	 */
	public static final String DEVICE_TYPE_GOUPIAOJI = "01";
	public static final String DEVICE_TYPE_ZHAJI = "02";
	
	public static enum DeviceType{
		DEVICE_TYPE_GOUPIAOJI("01", "购票机"), DEVICE_TYPE_ZHAJI("02","闸机");
		
		private DeviceType(String code, String name){
			this.code = code;
			this.name = name;
		}
		
		private String code;
		private String name;
		public String getCode() {
			return code;
		}
		public void setCode(String code) {
			this.code = code;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		
	}

}
