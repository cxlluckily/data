package com.shankephone.data.storage.pojo;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
public class CriteriaColumn extends TagElement {

	private static final long serialVersionUID = 1L;
	
	@XmlAttribute
	private String mysql;
	
	@XmlAttribute
	private String value;

	public String getMysql() {
		return mysql;
	}

	public void setMysql(String mysql) {
		this.mysql = mysql;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	

}
