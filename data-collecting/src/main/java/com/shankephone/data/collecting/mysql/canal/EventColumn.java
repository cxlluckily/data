package com.shankephone.data.collecting.mysql.canal;

import java.io.Serializable;

public class EventColumn implements Serializable {

    private static final long serialVersionUID = 8881024631437131042L;

    private int               columnType;

    private String            columnName;

    /**
     * timestamp,Datetime是一个long型的数字.
     */
    private String            columnValue;

	public int getColumnType() {
		return columnType;
	}

	public void setColumnType(int columnType) {
		this.columnType = columnType;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public String getColumnValue() {
		return columnValue;
	}

	public void setColumnValue(String columnValue) {
		this.columnValue = columnValue;
	}

}
