package com.shankephone.data.storage.pojo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement
public class ConfigTable extends TagElement {
	private static final long serialVersionUID = 1L;
	/**
	 * table元素的属性，表示需要同步的mysql源数据表
	 */
	@XmlAttribute
	private String mysql;
	/**
	 * table元素的属性，表示同步到hbase的目标数据表
	 */
	@XmlAttribute
	private String hbase;
	/**
	 * table元素的属性，表示同步到hbase表的列族
	 */
	@XmlAttribute
	private String columnFamily;
	/**
	 * 
	 * table元素的属性，表示该表对应的HBase中的历史表
	 */
	@XmlAttribute
	private String hbaseHist;
	/**
	 * table元素的属性， 如果值为空串或null，则自动同步源表中所有的列; 如果值为"specified"，则只同步配置的列
	 */
	@XmlAttribute
	private String scope;
	/**
	 * table元素的属性，数据要发布到kafka的topic名称
	 */
	@XmlAttribute
	private String topic;
	/**
	 * table元素的属性，HBase目标表中列名的前缀
	 */
	@XmlAttribute
	private String columnPrefix;
	/**
	 * table元素的子元素，生成一致性Hash的所有列，用于HBase分区散列
	 */
	@XmlElement(name = "hashkey")
	private HashKeys hashkeys;
	/**
	 * table元素的子元素，组成hbase表中的rowkey的列
	 */
	@XmlElement(name = "rowkey")
	private RowKeys rowkeys;
	/**
	 * table元素的子元素，用来保存所有的Column元素
	 */
	@XmlElement(name = "column")
	private List<Column> columns;

	/**
	 * table元素的子元素，数据的筛选条件，目前只支持‘与’的条件，表示满足条件则数据有效。
	 */
	@XmlElement(name = "criteria")
	private Criteria criteria;

	/**
	 * 列名与列的映射关系
	 */
	@XmlTransient
	private Map<String, List<Column>> columnsMap = new HashMap<>();

	@XmlTransient
	private Map<String, List<String>> originalColumnMap = new HashMap<String, List<String>>();

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

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getHbaseHist() {
		return hbaseHist;
	}

	public void setHbaseHist(String hbaseHist) {
		this.hbaseHist = hbaseHist;
	}

	public String getScope() {
		return scope;
	}

	public void setScope(String scope) {
		this.scope = scope;
	}

	public String getColumnPrefix() {
		return columnPrefix;
	}

	public void setColumnPrefix(String columnPrefix) {
		this.columnPrefix = columnPrefix;
	}

	public HashKeys getHashkeys() {
		return hashkeys;
	}

	public void setHashkeys(HashKeys hashkeys) {
		this.hashkeys = hashkeys;
	}

	public RowKeys getRowkeys() {
		return rowkeys;
	}

	public void setRowkeys(RowKeys rowkeys) {
		this.rowkeys = rowkeys;
	}

	public List<Column> getColumns() {
		return columns;
	}

	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}

	public Criteria getCriteria() {
		return criteria;
	}

	public void setCriteria(Criteria criteria) {
		this.criteria = criteria;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Map<String, List<Column>> getColumnsMap() {
		return columnsMap;
	}

	public void setColumnsMap(Map<String, List<Column>> columnsMap) {
		this.columnsMap = columnsMap;
	}

	public Map<String, List<String>> getOriginalColumnMap() {
		return originalColumnMap;
	}

	public void setOriginalColumnMap(Map<String, List<String>> originalColumnMap) {
		this.originalColumnMap = originalColumnMap;
	}

}
