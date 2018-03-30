package com.shankephone.data.collecting.config;

import java.io.FileNotFoundException;

import javax.xml.bind.JAXBException;

import org.junit.Test;

import com.alibaba.otter.canal.protocol.ClientIdentity;
import com.shankephone.data.common.test.TestCase;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.util.XmlUtils;

public class ConfigTests extends TestCase {
	
	@Test
	public void test() throws JAXBException, FileNotFoundException {
		Config config = XmlUtils.xml2Java("collectingConfig.xml", Config.class);
		System.out.println(config);
		
		System.out.println(config.getFilterRegex());
		
		System.out.println(config.getTableHashColumn("sttrade", "owner_order"));
		
		System.out.println(config.getKafkaTopic("sttrade", "owner_order"));
		
		
		ClientIdentity clientIdentity = new ClientIdentity();
    	clientIdentity.setDestination(PropertyAccessor.getProperty("canal.destination"));
    	clientIdentity.setClientId(Short.valueOf(PropertyAccessor.getProperty("canal.clientId")));
    	System.out.println(clientIdentity);
	}
	
	@Test
	public void testGetHashColumn(){
		Config config = XmlUtils.xml2Java("collectingConfig/dev.xml", Config.class);
	}
	
}
