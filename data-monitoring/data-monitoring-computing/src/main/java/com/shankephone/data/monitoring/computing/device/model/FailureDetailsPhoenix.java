package com.shankephone.data.monitoring.computing.device.model;



/**
 * HBASE中的shankephone:FAILURE_Details表
 * @author 森
 * @version 2017年10月27日 下午5:00:05
 */
public class FailureDetailsPhoenix{
	private String PK;
	private String CITY_CODE;
	private String CITY_NAME;
	private String LINE_CODE;
	private String LINE_NAME;
	private String STATION_CODE;
	private String STATION_NAME;
	private String DEVICE_ID;
	private String DEVICE_NAME;
	private String DEVICE_TYPE;
	private String DEVICE_TYPE_NAME;
	private String STATUS_ID;
	private String STATUS_VALUE;
	private String REASON;
	private String FAILURE_TIME;
	private String RECOVER_TIME;
	private String CREATE_TIME;
	
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
	public String getCITY_NAME() {
		return CITY_NAME;
	}
	public void setCITY_NAME(String cITY_NAME) {
		CITY_NAME = cITY_NAME;
	}
	public String getLINE_CODE() {
		return LINE_CODE;
	}
	public void setLINE_CODE(String lINE_CODE) {
		LINE_CODE = lINE_CODE;
	}
	public String getLINE_NAME() {
		return LINE_NAME;
	}
	public void setLINE_NAME(String lINE_NAME) {
		LINE_NAME = lINE_NAME;
	}
	public String getSTATION_CODE() {
		return STATION_CODE;
	}
	public void setSTATION_CODE(String sTATION_CODE) {
		STATION_CODE = sTATION_CODE;
	}
	public String getSTATION_NAME() {
		return STATION_NAME;
	}
	public void setSTATION_NAME(String sTATION_NAME) {
		STATION_NAME = sTATION_NAME;
	}
	public String getDEVICE_ID() {
		return DEVICE_ID;
	}
	public void setDEVICE_ID(String dEVICE_ID) {
		DEVICE_ID = dEVICE_ID;
	}
	public String getDEVICE_NAME() {
		return DEVICE_NAME;
	}
	public void setDEVICE_NAME(String dEVICE_NAME) {
		DEVICE_NAME = dEVICE_NAME;
	}
	public String getDEVICE_TYPE() {
		return DEVICE_TYPE;
	}
	public void setDEVICE_TYPE(String dEVICE_TYPE) {
		DEVICE_TYPE = dEVICE_TYPE;
	}
	public String getDEVICE_TYPE_NAME() {
		return DEVICE_TYPE_NAME;
	}
	public void setDEVICE_TYPE_NAME(String dEVICE_TYPE_NAME) {
		DEVICE_TYPE_NAME = dEVICE_TYPE_NAME;
	}
	public String getSTATUS_ID() {
		return STATUS_ID;
	}
	public void setSTATUS_ID(String sTATUS_ID) {
		STATUS_ID = sTATUS_ID;
	}
	public String getSTATUS_VALUE() {
		return STATUS_VALUE;
	}
	public void setSTATUS_VALUE(String sTATUS_VALUE) {
		STATUS_VALUE = sTATUS_VALUE;
	}
	public String getREASON() {
		return REASON;
	}
	public void setREASON(String rEASON) {
		REASON = rEASON;
	}
	public String getFAILURE_TIME() {
		return FAILURE_TIME;
	}
	public void setFAILURE_TIME(String fAILURE_TIME) {
		FAILURE_TIME = fAILURE_TIME;
	}
	public String getRECOVER_TIME() {
		return RECOVER_TIME;
	}
	public void setRECOVER_TIME(String rECOVER_TIME) {
		RECOVER_TIME = rECOVER_TIME;
	}
	public String getCREATE_TIME() {
		return CREATE_TIME;
	}
	public void setCREATE_TIME(String cREATE_TIME) {
		CREATE_TIME = cREATE_TIME;
	}
	
	

}
