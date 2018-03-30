package com.shankephone.data.monitoring.web.device.processor;

import net.minidev.json.JSONObject;

import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;



public class TimeInitProcessor implements SubDataInitProcessor {

	@Override
	public Object process(SubInfo subInfo) {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String lastTime = bucket.get();
		JSONObject json = new JSONObject();
		json.put("lastTime", lastTime);
		return json;
	}

}
