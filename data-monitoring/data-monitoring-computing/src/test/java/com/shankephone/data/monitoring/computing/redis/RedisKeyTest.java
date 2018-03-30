package com.shankephone.data.monitoring.computing.redis;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.redisson.api.RBucket;
import org.redisson.api.RTopic;
import org.redisson.api.RedissonClient;
import org.redisson.api.listener.MessageListener;
import org.redisson.client.codec.StringCodec;
import org.springframework.util.Assert;

import com.shankephone.data.cache.DeviceCache;
import com.shankephone.data.cache.StationCache;
import com.shankephone.data.common.redis.RedisUtils;

public class RedisKeyTest {
	
	@Test
	public void testKeys() {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		Assert.notNull(redisson.getBucket("heartbeat:4401:213113"), "为空！");
	}
	
	@Test
	public void testSubRLTICKET() throws IOException {
		RTopic<String> RTTICKET = RedisUtils.getRedissonClient().getTopic("device_failure_4401");
		RTTICKET.addListener((channel, msg) -> {
			System.out.println("channel:" + channel + ",msg:" + msg);
		});
		System.in.read();
	}
	
	@Test
	public void testSubRLTICKET1() throws IOException {
		RTopic<String> RTTICKET = RedisUtils.getRedissonClient().getTopic("device_failure_station_4401");
		RTTICKET.addListener((channel, msg) -> {
			System.out.println("channel:" + channel + ",msg:" + msg);
		});
		System.in.read();
	}
	
	@Test
	public void testListen(){
		RTopic<String> topic = RedisUtils.getRedissonClient().getTopic("__keyevent@0__:expired", StringCodec.INSTANCE);
		topic.addListener(new MessageListener<String>() {
			@Override
			public void onMessage(String channel, String msg) {
				System.out.println(msg);
			}
		});
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> bucket = redisson.getBucket("heartbeat:4401:0101");
		bucket.set("10");
		bucket.expire(10, TimeUnit.SECONDS);
		try {
			Thread.currentThread().sleep(20000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void testSubInitTime() throws IOException {
		String cityCode = "4401";
		String dev1 = "0104045545";
		String dev2 = "0417045229";
		String dev3 = "0516049325";
		String dev4 = "3011045145";
		String dev5 = "3011045445";
		
		String sta1 = dev1.substring(0,4);
		String sta2 = dev2.substring(0,4);
		String sta3 = dev3.substring(0,4);
		String sta4 = dev4.substring(0,4);
		String sta5 = dev5.substring(0,4);
		
		/*Map<String,String> m1 = DeviceCache.get(cityCode,dev1);
		Map<String,String> m2 = DeviceCache.get(cityCode,dev2);
		Map<String,String> m3 = DeviceCache.get(cityCode,dev3);
		Map<String,String> m4 = DeviceCache.get(cityCode,dev4);
		
		Map<String,String> m5 = DeviceCache.get(cityCode,dev5);
		Map<String,String> staMap5 = StationCache.get(cityCode, sta5);
		
		System.out.println(m5);
		System.out.println(staMap5);*/
		
		/*System.out.println(m1);
		System.out.println(m2);
		
		Map<String,String> staMap1 = StationCache.get(cityCode, sta1);
		Map<String,String> staMap2 = StationCache.get(cityCode, sta2);
		
		System.out.println(staMap1);
		System.out.println(staMap2);*/
		
	}

	
	@Test
	public void testSubStation() throws IOException {
		RTopic<String> RTTICKET = RedisUtils.getRedissonClient().getTopic("device_failure_station_");
		RTTICKET.addListener((channel, msg) -> {
			System.out.println("channel:" + channel + ",msg:" + msg);
		});
		System.in.read();
	}
}
