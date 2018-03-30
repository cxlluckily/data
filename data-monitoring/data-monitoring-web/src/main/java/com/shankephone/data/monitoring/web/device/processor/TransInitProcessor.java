package com.shankephone.data.monitoring.web.device.processor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.redisson.api.RedissonClient;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;


public class TransInitProcessor implements SubDataInitProcessor {

	@Override
	public Object process(SubInfo subInfo) {
		String topic = subInfo.getTopic();
		RedissonClient redisson = RedisUtils.getRedissonClient();
		JSONArray arr = new JSONArray();
		Map<String, Map<String, Integer>> cacheMap = redisson.getMap(topic);
		
		Set<String> set = cacheMap.keySet();
		String[] days = set.toArray(new String[set.size()]);
		Arrays.sort(days);
		for (int i = days.length - 1;  i >= 0 && i > days.length - 4; i--) {
			String day = days[i];
			JSONObject json = new JSONObject();
			json.put("day", day);
			if (day.equals(DateUtils.getCurrentDate())) {
				json.put("today", true);
			} else {
				json.put("today", false);
			}
			
			Map<String, Integer> dayMap = cacheMap.get(day);

			TreeMap<String, Integer> tree = new TreeMap<>(dayMap);
			List<String> timeList = new LinkedList<>(tree.keySet());
		
			JSONArray data = new JSONArray();
			for (String key : timeList) {
				String[] temp = {key, tree.get(key) + ""};
				data.add(temp);
			}
			json.put("time", timeList);
			json.put("value", data);
			arr.add(json);
		}
//		System.out.println("【arr】" + arr.toString());
		return arr;
	}

}
