package com.shankephone.data.cache;

import org.junit.Test;

import com.shankephone.data.common.test.SpringTestCase;

public class LineCacheTests extends SpringTestCase {

	@Test
	public void test() {
		LineCache.getInstance().initRedis();
	}

	
	@Test
	public void testGet() {
		System.out.println(LineCache.getInstance().get("4401", "01"));
	}
	
}
