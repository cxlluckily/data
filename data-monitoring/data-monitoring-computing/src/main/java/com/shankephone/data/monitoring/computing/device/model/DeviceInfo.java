package com.shankephone.data.monitoring.computing.device.model;

public class DeviceInfo {
	
	private String CITY_CODE;
	private String DEVICE_ID;
	private String STATION_CODE;
	private String LINE_CODE;
	private long T_TIMESTAMP;
	private long T_LAST_TIMESTAMP;
	
	public String getCITY_CODE() {
		return CITY_CODE;
	}
	public void setCITY_CODE(String cITY_CODE) {
		CITY_CODE = cITY_CODE;
	}
	public String getDEVICE_ID() {
		return DEVICE_ID;
	}
	public void setDEVICE_ID(String dEVICE_ID) {
		DEVICE_ID = dEVICE_ID;
	}
	public String getSTATION_CODE() {
		return STATION_CODE;
	}
	public void setSTATION_CODE(String sTATION_CODE) {
		STATION_CODE = sTATION_CODE;
	}
	public String getLINE_CODE() {
		return LINE_CODE;
	}
	public void setLINE_CODE(String lINE_CODE) {
		LINE_CODE = lINE_CODE;
	}
	public long getT_TIMESTAMP() {
		return T_TIMESTAMP;
	}
	public void setT_TIMESTAMP(long t_TIMESTAMP) {
		T_TIMESTAMP = t_TIMESTAMP;
	}
	public long getT_LAST_TIMESTAMP() {
		return T_LAST_TIMESTAMP;
	}
	public void setT_LAST_TIMESTAMP(long t_LAST_TIMESTAMP) {
		T_LAST_TIMESTAMP = t_LAST_TIMESTAMP;
	}
	
	

}
