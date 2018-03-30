package com.shankephone.data.common.hbase;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;

import com.shankephone.data.common.test.TestCase;

public class HBaseClientTests extends TestCase {
	
	@Test
	public void test() {
		HBaseClient.getInstance();
	}
	
	@Test
	public void testGetAll() {
		List<Map<String, String>> resultList = HBaseClient.getInstance().getAll("SHANKEPHONE:DEVICE_INFO");
		for (Map<String, String> rowMap : resultList) {
			for (Entry<String, String> entry : rowMap.entrySet()) {
				System.out.print(entry.getKey() + ":" + entry.getValue() + ", ");
			}
			System.out.println();
		}
	}
	
	@Test
	public void testGetAllClass() {
		List<DeviceStatusDic> resultList = HBaseClient.getInstance().getAll("SHANKEPHONE:DEVICE_STATUS_DIC", DeviceStatusDic.class);
		for (DeviceStatusDic result : resultList) {
			System.out.println(result);
		}
	}
	
}