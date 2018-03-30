package com.shankephone.data.storage.pojo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

/**
 * 配置文件的根元素
 * @author fengql
 * @version 2018年1月22日 上午11:47:12
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
public class Config extends TagElement{

	private static final long serialVersionUID = 1L;

	/**
	 * table元素列表
	 */
	@XmlElement(name="table")
	private List<ConfigTable> tables;
	
	/*
	 * table元素的Map
	 */
	@XmlTransient
	Map<String, List<ConfigTable>> tableMap = new HashMap<String, List<ConfigTable>>();
	
	/*
	 * 所有聚合的表
	 */
	@XmlTransient
	List<ConfigTable> aggregations = new ArrayList<ConfigTable>();
	
	/*
	 * 单表与历史表的Map
	 */
	@XmlTransient
	Map<String, String> historyMap = new HashMap<String, String>();
	
	public List<ConfigTable> getTables() {
		return tables;
	}

	public void setTables(List<ConfigTable> tables) {
		this.tables = tables;
	}
	
	public Map<String, List<ConfigTable>> getTableMap() {
		return tableMap;
	}
	public void setTableMap(Map<String, List<ConfigTable>> tableMap) {
		this.tableMap = tableMap;
	}
	public List<ConfigTable> getAggregations() {
		return aggregations;
	}
	public void setAggregations(List<ConfigTable> aggregations) {
		this.aggregations = aggregations;
	}
	public Map<String, String> getHistoryMap() {
		return historyMap;
	}
	public void setHistoryMap(Map<String, String> historyMap) {
		this.historyMap = historyMap;
	}
	
	
}
