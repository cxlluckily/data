package com.shankephone.data.visualization.web.ticket;

import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;



public class TicketTimeInitProcessor implements SubDataInitProcessor {

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
