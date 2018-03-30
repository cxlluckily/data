package com.shankephone.data.storage.spring;

import java.util.Properties;
import java.util.Set;

import org.junit.Test;

import com.shankephone.data.common.test.SpringTestCase;
import com.shankephone.data.common.util.PropertyAccessor;

public class PropertyAccessorTest extends SpringTestCase {
	
	@Test
	public void testGetProperty() {
		String property = PropertyAccessor.getProperty("spark.app.name");
		System.out.println(property);
//		assertEquals(property, "canal");
	}
	
	@Test
	public void testGetProperties(){
		Properties props = PropertyAccessor.getProperties("hbaseconf");
		Set<Object> keySet = props.keySet();
		for(Object obj : keySet){
			String key = (String)obj;
			System.out.println(key);
			System.out.println(props.getProperty(key));
			
		}
//		assertEquals(props.getProperty("client.id"), "canal");
	}
	
	
}
