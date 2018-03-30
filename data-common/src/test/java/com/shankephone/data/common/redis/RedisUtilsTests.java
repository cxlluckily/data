package com.shankephone.data.common.redis;

import org.junit.Test;
import org.redisson.api.RedissonClient;

import com.shankephone.data.common.test.SpringTestCase;

public class RedisUtilsTests extends SpringTestCase {
	
	@Test
	public void testGetRedissonClient() {
		RedissonClient redission = RedisUtils.getRedissonClient();
		assertNotNull(redission);
	}
	
	@Test
	public void testGetRedissonClient2() {
		RedissonClient redission = RedisUtils.getRedissonClient("redissonClient");
		assertNotNull(redission);
	}
}
