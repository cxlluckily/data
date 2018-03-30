package com.shankephone.data.collecting.config;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
public class Config implements Serializable {
	
	private static final long serialVersionUID = -3923131718440841746L;
	@XmlElement
	private String kafkaTopic;
	@XmlElement(name="table")
	private List<Table> tableList = new ArrayList<>();

	public String getKafkaTopic() {
		return kafkaTopic;
	}

	public void setKafkaTopic(String kafkaTopic) {
		this.kafkaTopic = kafkaTopic;
	}

	public List<Table> getTableList() {
		return tableList;
	}

	public void setTableList(List<Table> tableList) {
		this.tableList = tableList;
	}
	
	/**
	 * 采集的所有表的正则表达
	 * 若未设置采集的表，则采集所有表
	 * @author senze
	 * @date 2018年1月9日 下午3:08:51
	 * @return
	 */
	public String getFilterRegex() {
		String regex = "";
		for (Table t : tableList) {
			regex += t.getSchema() + "\\." + t.getName() + ",";
		}		
		return (tableList.size() <= 0) ?  ".*\\..*"
														: (regex.substring(0, regex.length() - 1));
	}
	
	/**
	 * 获取指定表的hashColumn
	 * 多个hashColumn以“,”分割
	 */
	public List<String> getTableHashColumn(String schemaName, String tableName) {
		List<String> hashColumns = new ArrayList<>();
		Table table = getTable(schemaName, tableName);
		
		return (table == null || table.getHashColumn() == null) 
					? hashColumns 
					: Arrays.asList(table.getHashColumn().split(","));
	}
	
	/**
	 * 获取指定table
	 */
	public Table getTable(String schemaName, String tableName) {
		String s = schemaName + "." + tableName;
		for (Table t : tableList) {
			if (s.matches(t.getSchema() + "\\." + t.getName())) {
				return t;
			}
		}
		return null;
	}
	
	/**
	 * 指定表的kafka topic 
	 */
	public String getKafkaTopic(String schemaName, String tableName) {
		String topic = getKafkaTopic();
		Table table = getTable(schemaName, tableName);
		
		return (table == null || table.getKafkaTopic() == null) 
					? topic 
					: table.getKafkaTopic();
	}

	@Override
	public String toString() {
		return String.format("Config [kafkaTopic=%s, tables=%s]", kafkaTopic, tableList);
	} 
	
}
