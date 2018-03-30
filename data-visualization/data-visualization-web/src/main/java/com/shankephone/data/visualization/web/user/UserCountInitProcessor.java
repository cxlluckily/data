package com.shankephone.data.visualization.web.user;

import org.redisson.api.RBucket;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.cache.City;
import com.shankephone.data.cache.CityCache;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.PropertyAccessor;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;
import com.shankephone.data.visualization.web.util.LastTimeUtils;

public class UserCountInitProcessor implements SubDataInitProcessor{

	@Override
	public Object process(SubInfo subInfo) {
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> timeBucket = redisson.getBucket("lastTime");
		String lasttime = timeBucket.get();
		if(lasttime == null || "".equals(lasttime)){
			return new JSONObject();
		}
		String current_date = LastTimeUtils.getLastTimeDate();
		String topics = subInfo.getTopic();
		String city_code = topics.substring(topics.lastIndexOf(":") + 1);
		City city = CityCache.getInstance().get(city_code);
		String city_name = city.getName();
		RSet<Object> set = redisson.getSet(topics + ":"+current_date);
		JSONObject result = new JSONObject();
		result.put("CITY_CODE", city_code);
		result.put("CITY_NAME", city_name);
		result.put("USER_COUNT", set.size());
		System.out.println(topics + "发布数据：" + result.toJSONString());
		return result;
	}

	

}
