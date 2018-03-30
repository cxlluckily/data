package com.shankephone.data.cache;

import java.io.Serializable;

/**
 * 线路信息
 * 
 * @author DuXiaohua
 * @version 2017年10月31日 下午4:24:07
 */
public class Line implements Serializable {

	private static final long serialVersionUID = -1750677738577526211L;

	private String cityCode;
	private String cityName;
	private String lineCode;
	private String lineNameZh;

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

	@Override
	public String toString() {
		return "Line [cityCode=" + cityCode + ", cityName=" + cityName + ", lineCode=" + lineCode + ", lineNameZh="
				+ lineNameZh + "]";
	}

}
