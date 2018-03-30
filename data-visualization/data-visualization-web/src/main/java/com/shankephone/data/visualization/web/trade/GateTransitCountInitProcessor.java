package com.shankephone.data.visualization.web.trade;

import java.util.Map;

import org.redisson.api.RMap;

import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;
import com.shankephone.data.visualization.web.util.LastTimeUtils;

public class GateTransitCountInitProcessor implements SubDataInitProcessor {
	
	public static final String GATETRANSIT_REDIS_KEY = "gate:vol:4401";
	
	@Override
	public Object process(SubInfo subInfo) {
		String lastTime = LastTimeUtils.getLastTimeDate();
		RMap<String, Map<String, Integer>> rmap = RedisUtils.getRedissonClient().getMap(GATETRANSIT_REDIS_KEY);
		Map<String, Integer> countMap = rmap.get(lastTime);
		return countMap;
	}

}
