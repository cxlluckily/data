package com.shankephone.data.cache;

import java.io.Serializable;

/**
 * 站点信息
 * 
 * @author DuXiaohua
 * @version 2017年10月31日 下午4:31:30
 */
public class Station implements Serializable {

	private static final long serialVersionUID = -7106321824358365933L;

	private String cityCode;
	private String cityName;
	private String lineCode;
	private String lineNameZh;
	private String stationCode;
	private String stationNameZh;

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

	@Override
	public String toString() {
		return "Station [cityCode=" + cityCode + ", cityName=" + cityName + ", lineCode=" + lineCode + ", lineNameZh="
				+ lineNameZh + ", stationCode=" + stationCode + ", stationNameZh=" + stationNameZh + "]";
	}

}
