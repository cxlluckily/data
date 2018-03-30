package com.shankephone.data.cache;

import java.io.Serializable;

/**
 * 城市代码
 * 
 * @author DuXiaohua
 * @version 2017年10月31日 上午10:58:38
 */
public class City implements Serializable {

	private static final long serialVersionUID = 1L;
	/**
	 * 城市代码
	 */
	private String code;
	/**
	 * 城市名称
	 */
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

	@Override
	public String toString() {
		return "City [code=" + code + ", name=" + name + "]";
	}

}