package com.shankephone.data.monitoring.computing.device.model;

import java.util.Date;

public class DeviceHeartBeat {
	
	private String PK;
	private String CITY_CODE;
	private String DEVICE_ID;
	private Date BEAT_DATE;
	private long T_TIMESTAMP;
	private long T_LAST_TIMESTAMP;
	public String getPK() {
		return PK;
	}
	public void setPK(String pK) {
		PK = pK;
	}
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
	public Date getBEAT_DATE() {
		return BEAT_DATE;
	}
	public void setBEAT_DATE(Date bEAT_DATE) {
		BEAT_DATE = bEAT_DATE;
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
