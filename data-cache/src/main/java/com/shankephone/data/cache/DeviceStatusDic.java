package com.shankephone.data.cache;

import java.io.Serializable;

/**
 * 设备状态字典
 * 
 * @author DuXiaohua
 * @version 2017年10月31日 下午3:57:35
 */
public class DeviceStatusDic implements Serializable {

	private static final long serialVersionUID = -8879493740547712627L;
	/**
	 * 状态类型，0：正常
	 */
	public static String statusType_normal = "0";
	/**
	 * 状态类型，1：异常
	 */
	public static String statusType_abnormal = "1";

	private String statusId;
	private String statusIdName;
	private String statusValue;
	private String statusValueName;
	/**
	 * 状态类型，0：正常，1：异常
	 */
	private String statusType;

	public String getStatusId() {
		return statusId;
	}

	public void setStatusId(String statusId) {
		this.statusId = statusId;
	}

	public String getStatusIdName() {
		return statusIdName;
	}

	public void setStatusIdName(String statusIdName) {
		this.statusIdName = statusIdName;
	}

	public String getStatusValue() {
		return statusValue;
	}

	public void setStatusValue(String statusValue) {
		this.statusValue = statusValue;
	}

	public String getStatusValueName() {
		return statusValueName;
	}

	public void setStatusValueName(String statusValueName) {
		this.statusValueName = statusValueName;
	}

	public String getStatusType() {
		return statusType;
	}

	public void setStatusType(String statusType) {
		this.statusType = statusType;
	}

	@Override
	public String toString() {
		return "DeviceStatusDic [statusId=" + statusId + ", statusIdName=" + statusIdName + ", statusValue="
				+ statusValue + ", statusValueName=" + statusValueName + ", statusType=" + statusType + "]";
	}

}
