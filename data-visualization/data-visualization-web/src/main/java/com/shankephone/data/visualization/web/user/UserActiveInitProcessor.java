package com.shankephone.data.visualization.web.user;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.shankephone.data.common.redis.RedisUtils;
import com.shankephone.data.common.util.DateUtils;
import com.shankephone.data.common.web.socket.SubDataInitProcessor;
import com.shankephone.data.common.web.socket.SubInfo;

public class UserActiveInitProcessor  implements SubDataInitProcessor {

	private static final String SEPERATOR = "_";
	private final static Logger logger = LoggerFactory.getLogger(UserActiveInitProcessor.class);
	
	@Override
	public Object process(SubInfo subInfo) {
		JSONObject json = null;
		
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RBucket<String> bucket = redisson.getBucket("lastTime");
		String time = bucket.get();
		if(time == null || "".equals(time)){
			return new JSONObject();
		}
		String topic = subInfo.getTopic();
		//从前端输入的topic参数格式为：topicname 或 topicname_citycode
		String city_code = "0000";
		//新老用户
		int idx = topic.lastIndexOf(":");
		city_code = topic.substring(idx + 1);
		String cacheName = topic.substring(0,idx);
		json = getAllUserActive(cacheName, city_code);
		logger.info("=================topic: " + topic + "=================");
		logger.info(json.toJSONString());
		return json;
	}
	
	
	
	/**
	 * 新老用户
	 * @author fengql
	 * @date 2017年8月31日 下午2:44:04
	 * @param topic
	 * @param city_code
	 * @return
	 */
	private JSONObject getAllUserActive(String topic ,String city_code){
		RedissonClient redisson = RedisUtils.getRedissonClient();
		RMap<String, Map<String,Integer>> rm = redisson.getMap(topic);
		Map<String,Integer> oldstr = rm.get(city_code + SEPERATOR + "2200");
		Map<String,Integer> newstr = rm.get(city_code + SEPERATOR + "2100");
		//生成30天的日期
		Date date = new Date();
		List<String> list = new ArrayList<String>();
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		for(int i = 0; i < 30; i++){
			cal.add(Calendar.DAY_OF_YEAR, -1);
			Date day = cal.getTime();
			String days = DateUtils.formatDate(day);
			list.add(days);
		}
		Collections.reverse(list);
		JSONObject json = new JSONObject();
		try {
			if(oldstr != null){
				JSONArray oldArray = new JSONArray();
				for (int i = 0; i < list.size(); i++) {
					String day = list.get(i);
					if(oldstr.get(day)==null){
						if(i!=0 && i!=list.size()-1){
							oldArray.add(0);
						}
					}else{
						oldArray.add(oldstr.get(day));
					}
				}
				json.put("olds", oldArray);
			}
			
			
			JSONArray xaxis = new JSONArray();
			if(newstr != null){
				JSONArray newArray = new JSONArray();
				for (int i = 0; i < list.size(); i++) {
					String day = list.get(i);
					if(newstr.get(day)==null){
						if(i!=0 && i!=list.size()-1){
							newArray.add(0);
						}
					}else{
						newArray.add(newstr.get(day));
					}
					xaxis.add(day);
				}
				json.put("xaxis", xaxis);
				json.put("news", newArray);
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("========user active data error: " + e.getMessage() + "======");
			return null;
		}
		System.out.println(json.toJSONString());
		return json;
	}

}
