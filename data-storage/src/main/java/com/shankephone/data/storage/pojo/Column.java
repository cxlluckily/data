package com.shankephone.data.storage.pojo;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * @todo mysql到hbase字段的映射关系
 * @author sunweihong
 * @version 2017年7月10日 上午10:54:57
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name="column")
public class Column extends TagElement {
	private static final long serialVersionUID = 1L;
	/**
	 * @todo mysql中字段
	 */
	@XmlAttribute
	private String mysql;
	/**
	 * @todo 对应hbase中的字段
	 */
	@XmlAttribute
	private String hbase;
	
	/**
	 * 指定列的常量值，用在Criteria元素的属性
	 */
	@XmlAttribute
	private String value;
	
	/**
	 * 是否使用前缀，true:使用，false:不使用
	 */
	@XmlAttribute
	private Boolean prefix;
	
	/**
	 * 获取的信息的Meta信息
	 */
	@XmlAttribute
	private String header;
	
	/**
	 * 自定义的处理类，用于规则处理
	 */
	@XmlAttribute
	private String handlerClass;
	
	/**
	 * 条件类型：MUST, SHOULD, NOT, 默认是MUST
	 */
	@XmlAttribute
	private String require;
	
	/**
	 * @todo 用来保存字段的映射关系
	 */
	@XmlTransient
	private Map<String,String> maps = new HashMap<String,String>();
	
	
	public String getMysql() {
		return mysql;
	}
	public void setMysql(String mysql) {
		this.mysql = mysql;
	}
	public String getHbase() {
		return hbase;
	}
	public void setHbase(String hbase) {
		this.hbase = hbase;
	}
	public Map<String, String> getMaps() {
		return maps;
	}
	public void setMaps(Map<String, String> maps) {
		this.maps = maps;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public Boolean getPrefix() {
		return prefix;
	}
	public void setPrefix(Boolean prefix) {
		this.prefix = prefix;
	}
	public String getHeader() {
		return header;
	}
	public void setHeader(String header) {
		this.header = header;
	}
	public String getHandlerClass() {
		return handlerClass;
	}
	public void setHandlerClass(String handlerClass) {
		this.handlerClass = handlerClass;
	}
	public String getRequire() {
		return require;
	}
	public void setRequire(String require) {
		this.require = require;
	}
	
}
