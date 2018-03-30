package com.shankephone.data.common.spark;

import org.junit.Test;

import com.shankephone.data.common.spark.config.StreamingConfig;
import com.shankephone.data.common.test.TestCase;
import com.shankephone.data.common.util.XmlUtils;

public class StreamingConfigTests extends TestCase {
	
	@Test
	public void test() {
		StreamingConfig config = XmlUtils.xml2Java("streamingConfig.xml", StreamingConfig.class);
		logger.debug(config.toString());
	}
}
