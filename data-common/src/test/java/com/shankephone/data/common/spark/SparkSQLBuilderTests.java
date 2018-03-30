package com.shankephone.data.common.spark;

import org.junit.Test;

import com.shankephone.data.common.test.SpringTestCase;

public class SparkSQLBuilderTests extends SpringTestCase {
	
	@Test
	public void testExecuteHBaseSQL() {
		SparkSQLBuilder builder = new SparkSQLBuilder("test");
		builder.executeHBaseSQL("order_info", "test.ftl", null).show();
	}
	
	@Test
	public void testExecuteSQL() {
		SparkSQLBuilder builder = new SparkSQLBuilder("test");
		builder.executeSQL("device_status", null).show();
	}
}
