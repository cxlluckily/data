package com.shankephone.data.cache;

import org.junit.Test;

import com.shankephone.data.common.test.SpringTestCase;

public class StationCacheTests extends SpringTestCase {

	@Test
	public void test() {
		StationCache.getInstance().initRedis();
	}

	
	@Test
	public void testGet() {
		System.out.println(StationCache.getInstance().get("4401", "0518"));
	}
	
}
