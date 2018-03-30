package com.shankephone.data.computing.spark;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.shankephone.data.common.spark.SparkStreamingExecutor;
import com.shankephone.data.common.test.SpringTestCase;

public class GateTransitCountTest extends SpringTestCase {
	private static final Logger logger = LoggerFactory.getLogger(GateTransitCountTest.class);
	@Test
	public void test() throws Exception {
		SparkStreamingExecutor.main(new String[]{"-appId", "tradeCount"});
	}
}
