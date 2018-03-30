package com.shankephone.data.cache;

import java.io.Serializable;

/**
 * 设备信息
 * 
 * @author DuXiaohua
 * @version 2017年10月31日 下午4:20:20
 */
public class Device implements Serializable {

	private static final long serialVersionUID = -5045768360770652905L;
	/**
	 * 设备状态，01：售票机
	 */
	public static final String DEVICETYPE_TICKET = "01";
	/**
	 * 设备状态，02：闸机
	 */
	public static final String DEVICETYPE_GATE = "02";
	private String cityCode;
	private String cityName;
	private String lineCode;
	private String lineNameZh;
	private String stationCode;
	private String stationNameZh;
	private String deviceId;
	private String deviceType;
	private String deviceName;
	private String areaCode;

	public String getCityCode() {
		return cityCode;
	}

	public void setCityCode(String cityCode) {
		this.cityCode = cityCode;
	}

	public String getCityName() {
		return cityName;
	}

	public void setCityName(String cityName) {
		this.cityName = cityName;
	}

	public String getLineCode() {
		return lineCode;
	}

	public void setLineCode(String lineCode) {
		this.lineCode = lineCode;
	}

	public String getLineNameZh() {
		return lineNameZh;
	}

	public void setLineNameZh(String lineNameZh) {
		this.lineNameZh = lineNameZh;
	}

	public String getStationCode() {
		return stationCode;
	}

	public void setStationCode(String stationCode) {
		this.stationCode = stationCode;
	}

	public String getStationNameZh() {
		return stationNameZh;
	}

	public void setStationNameZh(String stationNameZh) {
		this.stationNameZh = stationNameZh;
	}

	public String getDeviceId() {
		return deviceId;
	}

	public void setDeviceId(String deviceId) {
		this.deviceId = deviceId;
	}

	public String getDeviceType() {
		return deviceType;
	}

	public void setDeviceType(String deviceType) {
		this.deviceType = deviceType;
	}

	public String getDeviceName() {
		return deviceName;
	}

	public void setDeviceName(String deviceName) {
		this.deviceName = deviceName;
	}

	public String getAreaCode() {
		return areaCode;
	}

	public void setAreaCode(String areaCode) {
		this.areaCode = areaCode;
	}

	@Override
	public String toString() {
		return "Device [cityCode=" + cityCode + ", cityName=" + cityName + ", lineCode=" + lineCode + ", lineNameZh="
				+ lineNameZh + ", stationCode=" + stationCode + ", stationNameZh=" + stationNameZh + ", deviceId="
				+ deviceId + ", deviceType=" + deviceType + ", deviceName=" + deviceName + ", areaCode=" + areaCode
				+ "]";
	}

}
