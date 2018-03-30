package com.shankephone.data.computing.spark;

import org.junit.Test;

import com.shankephone.data.common.spark.SparkSQLBuilder;
import com.shankephone.data.common.test.SpringTestCase;

public class SparkSQLBuilderTests extends SpringTestCase {
	
	@Test
	public void testExecuteHBaseSQL() {
		SparkSQLBuilder builder = new SparkSQLBuilder("test");
		builder.executeHBaseSQL("order_info", "test", null).show();
	}
}
