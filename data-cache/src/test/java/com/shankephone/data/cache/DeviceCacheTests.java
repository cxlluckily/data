package com.shankephone.data.cache;

import org.junit.Test;

import com.shankephone.data.common.test.SpringTestCase;

public class DeviceCacheTests extends SpringTestCase {

	@Test
	public void test() {
		DeviceCache.getInstance().initRedis();
	}
	
	
	@Test
	public void testGet() {
		System.out.println(DeviceCache.getInstance().get("4401", "0102020032"));
	}
	
	@Test
	public void testGetByStation() {
		System.out.println(DeviceCache.getInstance().getByStation("4401", "0601"));
		System.out.println(DeviceCache.getInstance().getByStationName("4401", "体育西路"));
	}
}
