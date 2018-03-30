package com.shankephone.data.common.redis;

import org.redisson.api.RedissonClient;

import com.shankephone.data.common.spring.ContextAccessor;
/**
 * Redis相关工具类
 * @author DuXiaohua
 * @version 2017年8月4日 下午2:25:40
 */
public class RedisUtils {
	/**
	 * 获取spring上下文中唯一的RedissonClient
	 * @author DuXiaohua
	 * @date 2017年8月4日 下午2:25:54
	 * @return
	 */
	public static RedissonClient getRedissonClient() {
		return ContextAccessor.getBean("defaultRedissonClient" , RedissonClient.class);
	}
	
	public static RedissonClient getCacheRedissonClient() {
		return getRedissonClient("cacheRedissonClient");
	}
	
	/**
	 * 获取spring上下文中指定的RedissonClient
	 * @author DuXiaohua
	 * @date 2017年8月4日 下午2:28:13
	 * @param name
	 * @return
	 */
	public static RedissonClient getRedissonClient(String name) {
		return ContextAccessor.getBean(name, RedissonClient.class);
	}
	
}
