package com.shankephone.data.collecting.mysql.canal;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class EventData implements Serializable {

    private static final long serialVersionUID = -7071677425383765372L;
    
    private long            serverId;
    
    private String            tableName;

    private String            schemaName;
	
    private String            eventTypeName;
    
	private String            logfile;
	
	private long              logfileOffset;

    /**
     * 变更数据的业务时间.
     */
    private long              executeTime;

    /**
     * 变更后的主键值,如果是insert/delete变更前和变更后的主键值是一样的.
     */
    private Map<String, Object> keys;

    /**
     * 非主键的其他字段
     */
    private Map<String, Object> columns;
    
    private String sql;
    
	public long getServerId() {
		return serverId;
	}

	public void setServerId(long serverId) {
		this.serverId = serverId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getSchemaName() {
		return schemaName;
	}

	public void setSchemaName(String schemaName) {
		this.schemaName = schemaName;
	}

	public String getEventTypeName() {
		return eventTypeName;
	}

	public void setEventTypeName(String eventTypeName) {
		this.eventTypeName = eventTypeName;
	}

	public String getLogfile() {
		return logfile;
	}

	public void setLogfile(String logfile) {
		this.logfile = logfile;
	}

	public long getLogfileOffset() {
		return logfileOffset;
	}

	public void setLogfileOffset(long logfileOffset) {
		this.logfileOffset = logfileOffset;
	}

	public long getExecuteTime() {
		return executeTime;
	}

	public void setExecuteTime(long executeTime) {
		this.executeTime = executeTime;
	}

	public Map<String, Object> getKeys() {
		return keys;
	}

	public void setKeys(Map<String, Object> keys) {
		this.keys = keys;
	}

	public Map<String, Object> getColumns() {
		return columns;
	}

	public void setColumns(Map<String, Object> columns) {
		this.columns = columns;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

}
