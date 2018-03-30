package com.shankephone.data.storage.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

import com.shankephone.data.common.util.XmlUtils;
import com.shankephone.data.storage.pojo.ConfigTable;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name="config")
public class StorageConfig implements Configurable  {
	
	private static final long serialVersionUID = 1L;
	
	public static void main(String[] args) throws JAXBException {
		StorageConfig mappings = XmlUtils.xml2Java("storageConfig-test.xml", StorageConfig.class);
		XmlUtils.java2XmlString(mappings,  System.out);
	}
	
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
	List<String> aggregations = new ArrayList<String>();
	
	/**
	 * 单表原表
	 */
	@XmlTransient
	Map<String, ConfigTable> singleTableMap = new HashMap<String, ConfigTable>();
	
	/*
	 * 单表与历史表的Map
	 */
	@XmlTransient
	Map<String, ConfigTable> historyMap = new HashMap<String, ConfigTable>();
	
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
	public List<String> getAggregations() {
		return aggregations;
	}
	public void setAggregations(List<String> aggregations) {
		this.aggregations = aggregations;
	}
	public Map<String, ConfigTable> getHistoryMap() {
		return historyMap;
	}
	public void setHistoryMap(Map<String, ConfigTable> historyMap) {
		this.historyMap = historyMap;
	}

	public Map<String, ConfigTable> getSingleTableMap() {
		return singleTableMap;
	}

	public void setSingleTableMap(Map<String, ConfigTable> singleTableMap) {
		this.singleTableMap = singleTableMap;
	}
	
	


}
