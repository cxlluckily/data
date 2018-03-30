package com.shankephone.data.cache;

import org.junit.Test;

import com.shankephone.data.common.test.SpringTestCase;

public class DeviceStatusDicCacheTests extends SpringTestCase {

	@Test
	public void test() {
		DeviceStatusDicCache.getInstance().initRedis();
	}

	@Test
	public void testGetAll() {
		System.out.println(DeviceStatusDicCache.getInstance().getAll());
	}
	
	@Test
	public void testGet() {
		System.out.println(DeviceStatusDicCache.getInstance().get("0000", "4"));
	}
	
}
