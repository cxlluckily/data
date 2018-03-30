package com.shankephone.data.common.util;

import java.util.Properties;

import org.junit.Test;

import com.shankephone.data.common.test.TestCase;
import com.shankephone.data.common.util.PropertyAccessor;

public class PropertyAccessorTest2 extends TestCase {
	
	@Test
	public void testGetProperty() {
		String property = PropertyAccessor.getProperty("canal.producer.kafka.client.id");
		assertEquals("canal", property);
	}
	
	@Test
	public void testGetProperties(){
		Properties props = PropertyAccessor.getProperties("default.producer.kafka");
		assertEquals("1", props.getProperty("linger.ms"));
	}
	
}
