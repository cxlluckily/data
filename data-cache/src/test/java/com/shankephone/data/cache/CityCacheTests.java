package com.shankephone.data.cache;

import org.junit.Test;

import com.shankephone.data.common.test.SpringTestCase;

public class CityCacheTests extends SpringTestCase {

	@Test
	public void test() {
		CityCache.getInstance().initRedis();
	}

	@Test
	public void testGetAll() {
		System.out.println(CityCache.getInstance().getAll());
	}
	
	@Test
	public void testGet() {
		System.out.println(CityCache.getInstance().get("4401"));
	}
	
}
